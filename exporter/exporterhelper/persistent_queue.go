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
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/extension/storage"
)

// persistentQueue holds the queue backed by file storage
type persistentQueue struct {
	logger     *zap.Logger
	stopWG     sync.WaitGroup
	produceMu  sync.Mutex
	exitChan   chan struct{}
	capacity   int
	numWorkers int

	storage persistentStorage
}

// persistentStorage provides an interface for request storage operations
type persistentStorage interface {
	// put appends the request to the storage
	put(req request) error
	// get returns the next available request; not the channel is unbuffered
	get() <-chan request
	// size returns the current size of the storage in number of requets
	size() int
	// stop gracefully stops the storage
	stop()
}

// persistentContiguousStorage provides a persistent queue implementation backed by file storage extension
//
// Write index describes the position at which next item is going to be stored.
// Read index describes which item needs to be read next.
// When Write index = Read index, no elements are in the queue.
//
// The items currently processed by consumers are not deleted until the processing is finished.
// Their list is stored under a separate key.
//
//
//   ┌───────file extension-backed queue───────┐
//   │                                         │
//   │     ┌───┐     ┌───┐ ┌───┐ ┌───┐ ┌───┐   │
//   │ n+1 │ n │ ... │ 4 │ │ 3 │ │ 2 │ │ 1 │   │
//   │     └───┘     └───┘ └─x─┘ └─|─┘ └─x─┘   │
//   │                       x     |     x     │
//   └───────────────────────x─────|─────x─────┘
//      ▲              ▲     x     |     x
//      │              │     x     |     xxxx deleted
//      │              │     x     |
//    write          read    x     └── currently processed item
//    index          index   x
//                           xxxx deleted
//
type persistentContiguousStorage struct {
	logger                  *zap.Logger
	client                  storage.Client
	mu                      sync.Mutex
	queueName               string
	readIndex               itemIndex
	writeIndex              itemIndex
	retryDelay              time.Duration
	unmarshaler             requestUnmarshaler
	reqChan                 chan request
	stopChan                chan struct{}
	stopOnce                sync.Once
	currentlyProcessedItems []itemIndex
}

type itemIndex uint64

const (
	zapKey           = "key"
	zapQueueNameKey  = "queueName"
	zapErrorCount    = "errorCount"
	zapNumberOfItems = "numberOfItems"

	defaultRetryDelay = 100 * time.Millisecond

	readIndexKey               = "ri"
	writeIndexKey              = "wi"
	currentlyProcessedItemsKey = "pi"
)

var (
	errStoringItemToQueue = errors.New("item could not be stored to persistent queue")
	errUpdatingIndex      = errors.New("index could not be updated")
	errItemNotFound = errors.New("item was not found")
)

// newPersistentQueue creates a new queue backed by file storage
func newPersistentQueue(ctx context.Context, name string, capacity int, logger *zap.Logger, client storage.Client, unmarshaler requestUnmarshaler) *persistentQueue {
	return &persistentQueue{
		logger:   logger,
		exitChan: make(chan struct{}),
		capacity: capacity,
		storage:  newPersistentContiguousStorage(ctx, name, logger, client, unmarshaler),
	}
}

// StartConsumers starts the given number of consumers which will be consuming items
func (pq *persistentQueue) StartConsumers(num int, callback func(item interface{})) {
	pq.numWorkers = num
	var startWG sync.WaitGroup

	factory := func() queue.Consumer {
		return queue.ConsumerFunc(callback)
	}

	for i := 0; i < pq.numWorkers; i++ {
		pq.stopWG.Add(1)
		startWG.Add(1)
		go func() {
			startWG.Done()
			defer pq.stopWG.Done()
			consumer := factory()

			for {
				select {
				case req := <-pq.storage.get():
					consumer.Consume(req)
				case <-pq.exitChan:
					return
				}
			}
		}()
	}
	startWG.Wait()
}

// Produce adds an item to the queue and returns true if it was accepted
func (pq *persistentQueue) Produce(item interface{}) bool {
	pq.produceMu.Lock()
	defer pq.produceMu.Unlock()

	if pq.storage.size() >= pq.capacity {
		return false
	}
	err := pq.storage.put(item.(request))
	return err == nil
}

// Stop stops accepting items, shuts down the queue and closes the persistent queue
func (pq *persistentQueue) Stop() {
	pq.storage.stop()
	close(pq.exitChan)
	pq.stopWG.Wait()
}

// Size returns the current depth of the queue
func (pq *persistentQueue) Size() int {
	return pq.storage.size()
}

// newPersistentContiguousStorage creates a new file-storage extension backed queue. It needs to be initialized separately
func newPersistentContiguousStorage(ctx context.Context, queueName string, logger *zap.Logger, client storage.Client, unmarshaler requestUnmarshaler) *persistentContiguousStorage {
	wcs := &persistentContiguousStorage{
		logger:      logger,
		client:      client,
		queueName:   queueName,
		unmarshaler: unmarshaler,
		reqChan:     make(chan request),
		stopChan:    make(chan struct{}),
		retryDelay:  defaultRetryDelay,
	}
	initPersistentContiguousStorage(ctx, wcs)
	go wcs.loop()
	return wcs
}

func initPersistentContiguousStorage(ctx context.Context, wcs *persistentContiguousStorage) {
	readIndexIf := wcs._clientGet(ctx, readIndexKey, bytesToItemIndex)
	if readIndexIf == nil {
		wcs.logger.Debug("failed getting read index, starting with a new one",
			zap.String(zapQueueNameKey, wcs.queueName))
		wcs.readIndex = 0
	} else {
		wcs.readIndex = readIndexIf.(itemIndex)
	}

	writeIndexIf := wcs._clientGet(ctx, writeIndexKey, bytesToItemIndex)
	if writeIndexIf == nil {
		wcs.logger.Debug("failed getting write index, starting with a new one",
			zap.String(zapQueueNameKey, wcs.queueName))
		wcs.writeIndex = 0
	} else {
		wcs.writeIndex = writeIndexIf.(itemIndex)
	}
}

// loop is the main loop that handles fetching items from the persistent buffer
func (pcs *persistentContiguousStorage) loop() {
	// We want to run it here so it's not blocking
	pcs.loadLeftProcessedItems(context.Background())

	for {
		for {
			req, found := pcs.getNextItem(context.Background())
			if found {
				select {
				case <-pcs.stopChan:
					return
				case pcs.reqChan <- req:
				}
			} else {
				select {
				case <-pcs.stopChan:
					return
				case <-time.After(pcs.retryDelay):
				}
			}
		}
	}
}

// get returns the request channel that all the requests will be send on
func (pcs *persistentContiguousStorage) get() <-chan request {
	return pcs.reqChan
}

func (pcs *persistentContiguousStorage) size() int {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()
	return int(pcs.writeIndex - pcs.readIndex)
}

func (pcs *persistentContiguousStorage) stop() {
	pcs.logger.Debug("stopping persistentContiguousStorage", zap.String(zapQueueNameKey, pcs.queueName))
	pcs.stopOnce.Do(func() {
		close(pcs.stopChan)
	})
}

// Put marshals the request and puts it into the persistent queue
func (pcs *persistentContiguousStorage) put(req request) error {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()

	ctx := context.Background()

	itemKey := pcs.buildItemKey(pcs.writeIndex)
	if pcs._clientSet(ctx, itemKey, req, requestToBytes) == false {
		return errStoringItemToQueue
	}

	pcs.writeIndex++
	if pcs._clientSet(ctx, writeIndexKey, pcs.writeIndex, itemIndexToBytes) == false {
		return errUpdatingIndex
	}

	return nil
}

// getNextItem pulls the next available item from the persistent storage; if none is found, returns (nil, false)
func (pcs *persistentContiguousStorage) getNextItem(ctx context.Context) (request, bool) {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()

	if pcs.readIndex < pcs.writeIndex {
		itemIndex := pcs.readIndex
		itemKey := pcs.buildItemKey(itemIndex)
		// Increase here, so despite errors it would still progress
		pcs.readIndex++

		pcs.proceedToNextItem(ctx)
		pcs.markProcessedItem(ctx, itemIndex)

		req := pcs._clientGet(ctx, itemKey, pcs.bytesToRequest).(request)
		if req == nil {
			return nil, false
		}

		req.setOnProcessingFinished(func() {
			pcs.removeProcessedItem(ctx, itemIndex)
		})
		return req, true
	}

	return nil, false
}

// loadLeftProcessedItems gets the unprocessed items from the persistent queues and moves them back to the queue
func (pcs *persistentContiguousStorage) loadLeftProcessedItems(ctx context.Context) {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()

	pcs.logger.Debug("checking if there are items left by consumers")
	processedItemsIf := pcs._clientGet(ctx, currentlyProcessedItemsKey, bytesToItemIndexArray)
	if processedItemsIf == nil {
		return
	}
	processedItems := processedItemsIf.([]itemIndex)

	if len(processedItems) > 0 {
		pcs.logger.Info("fetching items left for processing by consumers",
			zap.String(zapQueueNameKey, pcs.queueName), zap.Int(zapNumberOfItems, len(processedItems)))
	} else {
		pcs.logger.Debug("no items left for processing by consumers")
	}

	errCount := 0
	for _, it := range processedItems {
		req := pcs._clientGet(ctx, pcs.buildItemKey(it), pcs.bytesToRequest).(request)
		pcs._clientDelete(ctx, pcs.buildItemKey(it))
		if pcs.put(req) != nil {
			errCount++
		}
	}

	if len(processedItems) > 0 {
		pcs.logger.Info("moved items for processing back to queue",
			zap.String(zapQueueNameKey, pcs.queueName),
			zap.Int(zapNumberOfItems, len(processedItems)), zap.Int(zapErrorCount, errCount))
	}
}

// markProcessedItem appends the item to the list of currently processed items
func (pcs *persistentContiguousStorage) markProcessedItem(ctx context.Context, index itemIndex) {
	pcs.currentlyProcessedItems = append(pcs.currentlyProcessedItems, index)
	pcs._clientSet(ctx, currentlyProcessedItemsKey, pcs.currentlyProcessedItems, itemIndexArrayToBytes)
}

// removeProcessedItem removes the item from the list of currently processed items and deletes it from the persistent queue
func (pcs *persistentContiguousStorage) removeProcessedItem(ctx context.Context, index itemIndex) {
	var updatedProcessedItems []itemIndex
	for _, it := range pcs.currentlyProcessedItems {
		if it != index {
			updatedProcessedItems = append(updatedProcessedItems, index)
		}
	}
	pcs.currentlyProcessedItems = updatedProcessedItems

	pcs._clientSet(ctx, currentlyProcessedItemsKey, pcs.currentlyProcessedItems, itemIndexArrayToBytes)
	pcs._clientDelete(ctx, pcs.buildItemKey(index))
}

func (pcs *persistentContiguousStorage) proceedToNextItem(ctx context.Context) {
	pcs._clientSet(ctx, readIndexKey, pcs.readIndex, itemIndexToBytes)
}

func (pcs *persistentContiguousStorage) buildItemKey(index itemIndex) string {
	return fmt.Sprintf("%d", index)
}

func (pcs *persistentContiguousStorage) _clientSet(ctx context.Context, key string, value interface{}, marshal func(interface{}) ([]byte, error)) bool {
	valueBytes, err := marshal(value)
	if err != nil {
		pcs.logger.Warn("failed marshaling item",
			zap.String(zapQueueNameKey, pcs.queueName), zap.String(zapKey, key), zap.Error(err))
		return false
	}

	return pcs._clientSetBuf(ctx, key, valueBytes)
}

func (pcs *persistentContiguousStorage) _clientGet(ctx context.Context, key string, unmarshal func([]byte) (interface{}, error)) interface{} {
	valueBytes := pcs._clientGetBuf(ctx, key)
	if valueBytes == nil {
		return nil
	}

	item, err := unmarshal(valueBytes)
	if err != nil {
		pcs.logger.Warn("failed unmarshaling item",
			zap.String(zapQueueNameKey, pcs.queueName), zap.String(zapKey, key), zap.Error(err))
		return nil
	}

	return item
}

func (pcs *persistentContiguousStorage) _clientDelete(ctx context.Context, key string) bool {
	err := pcs.client.Delete(ctx, key)
	if err != nil {
		pcs.logger.Warn("failed deleting item",
			zap.String(zapQueueNameKey, pcs.queueName), zap.String(zapKey, key))
		return false
	}
	return true
}

func (pcs *persistentContiguousStorage) _clientGetBuf(ctx context.Context, key string) []byte {
	buf, err := pcs.client.Get(ctx, key)
	if err != nil {
		pcs.logger.Debug("error when getting item from persistent storage",
			zap.String(zapQueueNameKey, pcs.queueName), zap.String(zapKey, key), zap.Error(err))
		return nil
	}
	return buf
}

func (pcs *persistentContiguousStorage) _clientSetBuf(ctx context.Context, key string, buf []byte) bool {
	err := pcs.client.Set(ctx, key, buf)
	if err != nil {
		pcs.logger.Debug("error when storing item to persistent storage",
			zap.String(zapQueueNameKey, pcs.queueName), zap.String(zapKey, key), zap.Error(err))
		return false
	}
	return true
}

func itemIndexToBytes(val interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func bytesToItemIndex(b []byte) (interface{}, error) {
	var val itemIndex
	err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &val)
	if err != nil {
		return val, err
	}
	return val, nil
}

func itemIndexArrayToBytes(arr interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, arr)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func bytesToItemIndexArray(b []byte) (interface{}, error) {
	var val []itemIndex
	err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &val)
	if err != nil {
		return val, err
	}
	if val == nil {
		return []itemIndex{}, nil
	}
	return val, nil
}

func requestToBytes(req interface{}) ([]byte, error) {
	return req.(request).marshal()
}

func (pcs *persistentContiguousStorage) bytesToRequest(b []byte) (interface{}, error) {
	return pcs.unmarshaler(b)
}
