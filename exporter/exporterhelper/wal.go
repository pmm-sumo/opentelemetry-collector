// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exporterhelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jaegertracing/jaeger/pkg/queue"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/extension/storage"
)

// WALQueue holds the WAL-backed queue
type WALQueue struct {
	logger     *zap.Logger
	numWorkers int
	stopWG     sync.WaitGroup
	stopped    *atomic.Uint32
	storage    walStorage
}

type walStorage interface {
	put(ctx context.Context, req request) error
	get(ctx context.Context) (request, error)
	size() int
	stop()
}

// walContiguousStorage provides a WAL implementation backed by file storage extension
//
// Write index describes the position at which next item is going to be stored
// Read index describes which item needs to be read next
// When Write index = Read index, no elements are in the queue
//
//   ┌───────WAL-backed queue──────┐
//   │                             │
//   │     ┌───┐     ┌───┐ ┌───┐   │
//   │ n+1 │ n │ ... │ 4 │ │ 3 │   │
//   │     └───┘     └───┘ └─x─┘   │
//   │                       x     │
//   └───────────────────────x─────┘
//      ▲              ▲     x
//      │              │     xxx deleted
//      │              │
//    write          read
//    index          index
//
type walContiguousStorage struct {
	logger        *zap.Logger
	client        storage.Client
	mu            sync.Mutex
	id            string
	readIndex     uint64
	writeIndex    uint64
	readIndexKey  string
	writeIndexKey string
	unmarshaler   requestUnmarshaler
}

const (
	zapItemKey         = "itemKey"
	zapWalIDKey        = "walID"
	itemKeyTemplate    = "%s-it-%d"
	readIndexTemplate  = "%s-ri"
	writeIndexTemplate = "%s-wi"
)

var (
	errNotFound = errors.New("not found")
)

func newWALQueue(ctx context.Context, id string, logger *zap.Logger, client storage.Client, unmarshaler requestUnmarshaler) *WALQueue {
	return &WALQueue{
		logger:  logger,
		stopped: atomic.NewUint32(0),
		storage: newWALContiguousStorage(ctx, id, logger, client, unmarshaler),
	}
}

// StartConsumers starts the given number of consumers which will be consuming items
func (wq *WALQueue) StartConsumers(num int, callback func(item interface{})) {
	wq.numWorkers = num
	var startWG sync.WaitGroup

	factory := func() queue.Consumer {
		return queue.ConsumerFunc(callback)
	}

	for i := 0; i < wq.numWorkers; i++ {
		wq.stopWG.Add(1)
		startWG.Add(1)
		go func() {
			startWG.Done()
			defer wq.stopWG.Done()
			consumer := factory()

			for {
				if wq.stopped.Load() != 0 {
					return
				}
				req, err := wq.storage.get(context.Background())
				if err == errNotFound || req == nil {
					time.Sleep(1 * time.Second)
				} else {
					consumer.Consume(req)
				}
			}
		}()
	}
	startWG.Wait()
}

// Produce adds an item to the queue and returns true if it was accepted
func (wq *WALQueue) Produce(item interface{}) bool {
	if wq.stopped.Load() != 0 {
		return false
	}

	err := wq.storage.put(context.Background(), item.(request))
	return err == nil
}

// Stop stops accepting items and shuts-down the queue and closes the WAL
func (wq *WALQueue) Stop() {
	wq.storage.stop()
	wq.stopped.Store(1)
	wq.stopWG.Wait()
}

// Size returns the current depth of the queue
func (wq *WALQueue) Size() int {
	return wq.storage.size()
}

// newWALContiguousStorage creates a new WAL backed by file storage extension. It needs to be initialized separately
func newWALContiguousStorage(ctx context.Context, id string, logger *zap.Logger, client storage.Client, unmarshaler requestUnmarshaler) *walContiguousStorage {
	wcs := &walContiguousStorage{
		id:          id,
		logger:      logger,
		client:      client,
		unmarshaler: unmarshaler,
	}
	initWALContiguousStorage(ctx, wcs)
	return wcs
}

func initWALContiguousStorage(ctx context.Context, wcs *walContiguousStorage) {
	wcs.readIndexKey = wcs.buildReadIndexKey()
	wcs.writeIndexKey = wcs.buildWriteIndexKey()

	readIndexBytes, err := wcs.client.Get(ctx, wcs.readIndexKey)
	if err != nil || readIndexBytes == nil {
		wcs.logger.Debug("failed getting read index, starting with a new one", zap.String(zapWalIDKey, wcs.id))
		wcs.readIndex = 0
	} else {
		val, conversionErr := bytesToUint64(readIndexBytes)
		if conversionErr != nil {
			// TODO: consider failing here?
			wcs.logger.Warn("read index corrupted, starting with a new one", zap.String(zapWalIDKey, wcs.id))
			wcs.readIndex = 0
		} else {
			wcs.readIndex = val
		}
	}

	writeIndexBytes, err := wcs.client.Get(ctx, wcs.writeIndexKey)
	if err != nil || writeIndexBytes == nil {
		wcs.logger.Debug("failed getting write index, starting with a new one", zap.String(zapWalIDKey, wcs.id))
		wcs.writeIndex = 0
	} else {
		val, conversionErr := bytesToUint64(writeIndexBytes)
		if conversionErr != nil {
			// TODO: consider failing here?
			wcs.logger.Warn("write index corrupted, starting with a new one", zap.String(zapWalIDKey, wcs.id))
			wcs.writeIndex = 0
		} else {
			wcs.writeIndex = val
		}
	}
}

// put marshals the request and puts it into the WAL
func (wcs *walContiguousStorage) put(ctx context.Context, req request) error {
	buf, err := req.marshall()
	if err != nil {
		return err
	}

	wcs.mu.Lock()
	defer wcs.mu.Unlock()

	itemKey := wcs.buildItemKey(wcs.writeIndex)
	err = wcs.client.Set(ctx, itemKey, buf)
	if err != nil {
		return err
	}

	wcs.writeIndex++
	writeIndexBytes, err := uint64ToBytes(wcs.writeIndex)
	if err != nil {
		wcs.logger.Warn("failed converting write index uint64 to bytes", zap.Error(err))
	}

	return wcs.client.Set(ctx, wcs.writeIndexKey, writeIndexBytes)
}

// get returns the next request from the queue; note that it might be blocking if there are no entries in the WAL
func (wcs *walContiguousStorage) get(ctx context.Context) (request, error) {
	wcs.mu.Lock()
	defer wcs.mu.Unlock()

	if wcs.readIndex >= wcs.writeIndex {
		return nil, errNotFound
	}

	itemKey := wcs.buildItemKey(wcs.readIndex)
	// Increase here, so despite errors it would still progress
	wcs.readIndex++

	buf, err := wcs.client.Get(ctx, itemKey)
	if err != nil {
		// This is the only critical error
		return nil, err
	}

	// This needs to happen after unmarshalling as buf contents might change
	defer wcs.updateItemRead(ctx, itemKey)

	return wcs.unmarshaler(buf)
}

func (wcs *walContiguousStorage) size() int {
	return int(wcs.writeIndex - wcs.readIndex)
}

func (wcs *walContiguousStorage) stop() {
	wcs.logger.Debug("stopping walContiguousStorage", zap.String(zapWalIDKey, wcs.id))
}

func (wcs *walContiguousStorage) updateItemRead(ctx context.Context, itemKey string) {
	err := wcs.client.Delete(ctx, itemKey)
	if err != nil {
		wcs.logger.Debug("failed deleting item", zap.String(zapItemKey, itemKey))
	}

	readIndexBytes, err := uint64ToBytes(wcs.readIndex)
	if err != nil {
		wcs.logger.Warn("failed converting read index uint64 to bytes", zap.Error(err))
	} else {
		err = wcs.client.Set(ctx, wcs.readIndexKey, readIndexBytes)
		if err != nil {
			wcs.logger.Warn("failed storing read index", zap.Error(err))
		}
	}
}

func (wcs *walContiguousStorage) buildItemKey(index uint64) string {
	return fmt.Sprintf(itemKeyTemplate, wcs.id, index)
}

func (wcs *walContiguousStorage) buildReadIndexKey() string {
	return fmt.Sprintf(readIndexTemplate, wcs.id)
}

func (wcs *walContiguousStorage) buildWriteIndexKey() string {
	return fmt.Sprintf(writeIndexTemplate, wcs.id)
}

func uint64ToBytes(val uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func bytesToUint64(b []byte) (uint64, error) {
	var val uint64
	err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &val)
	if err != nil {
		return val, err
	}
	return val, nil
}
