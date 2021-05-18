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
	"fmt"
	"github.com/jaegertracing/jaeger/pkg/queue"
	"github.com/tidwall/wal"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"path/filepath"
	"sync"
	"time"
)

type WalQueue struct {
	logger      *zap.Logger
	workers     int
	stopWG      sync.WaitGroup
	walMu       sync.Mutex
	wal         *wal.Log
	stopped     *atomic.Uint32
	quit 		chan struct{}
	// This is first item to read
	readIndex   uint64
	unmarshaler requestUnmarshaler
}

func newWalQueue(logger *zap.Logger, path string, syncFrequency time.Duration, unmarshaler requestUnmarshaler) *WalQueue {
	wq, err := createWalQueue(logger, path, syncFrequency, unmarshaler)
	if err != nil {
		panic(err)
	}
	return wq
}

func createWalQueue(logger *zap.Logger, path string, syncFrequency time.Duration, unmarshaler requestUnmarshaler) (*WalQueue, error) {
	noSyncOption := syncFrequency > 0

	_wal, err := wal.Open(filepath.Join(path, "wal"), &wal.Options{
		SegmentCacheSize: 300,
		NoCopy:           true,
		NoSync: 		  noSyncOption,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	firstIndex, err := _wal.FirstIndex()
	if err != nil {
		logger.Error("Error when reading FirstIndex", zap.Error(err))
		return nil, err
	}

	wq := &WalQueue{
		logger: 	 logger,
		wal:         _wal,
		unmarshaler: unmarshaler,
		stopped:     atomic.NewUint32(0),
		readIndex:  firstIndex,
	}

	if noSyncOption {
		wq.quit = wq.startPeriodicSync(syncFrequency)
	}

	return wq, nil
}

func (wq *WalQueue) startPeriodicSync(syncFrequency time.Duration) chan struct{} {
	wq.logger.Info("Starting periodic sync", zap.Duration("sync-frequency", syncFrequency))
	ticker := time.NewTicker(syncFrequency)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:

				err := wq.sync()
				if err == wal.ErrNotFound {
					// This can happen on init
					time.Sleep(1 * time.Second)
					// TODO: remove this
					wq.logger.Debug("Error while doing periodic sync", zap.Error(err))
				} else if err != nil {
					wq.logger.Error("Error while doing periodic sync", zap.Error(err))
				}
			case <-quit:
				wq.logger.Info("Stopping periodic sync")
				ticker.Stop()
				return
			}
		}
	}()

	return quit
}

func (wq *WalQueue) writeIndex() uint64 {
	index, err := wq.wal.LastIndex()
	if err != nil {
		wq.logger.Error("Error when reading LastIndex", zap.Error(err))
		panic(err)
	}
	return index
}


func (wq *WalQueue) StartConsumers(num int, callback func(item interface{})) {
	wq.workers = num
	var startWG sync.WaitGroup

	factory := func() queue.Consumer {
		return queue.ConsumerFunc(callback)
	}

	for i := 0; i < wq.workers; i++ {
		wq.stopWG.Add(1)
		startWG.Add(1)
		go func() {
			startWG.Done()
			defer wq.stopWG.Done()
			consumer := factory()

			for {
				if wq.stopped.Load() == 1 {
					return
				}
				req, err := wq.get()
				if err == wal.ErrNotFound {
					time.Sleep(1 * time.Second)
				} else {
					consumer.Consume(req)
				}
			}
		}()
	}
	startWG.Wait()
}

func (wq *WalQueue) Produce(item interface{}) bool {
	if wq.stopped.Load() != 0 {
		return false
	}

	err := wq.put(item.(request))
	return err == nil
}

func (wq *WalQueue) Stop() {
	wq.logger.Debug("Stopping WAL")
	wq.stopped.Store(1)
	if wq.quit != nil {
		close(wq.quit)
	}
	wq.stopWG.Wait()
	err := wq.sync()
	if err != nil {
		wq.logger.Error("Error when syncing WAL", zap.Error(err))
	}
	err = wq.close()
	if err != nil {
		wq.logger.Error("Error when closing WAL", zap.Error(err))
	}
}

func (wq *WalQueue) Size() int {
	wq.walMu.Lock()
	defer wq.walMu.Unlock()
	if wq.readIndex < 1 {
		return 0
	}
	return int(wq.writeIndex())-int(wq.readIndex)+1
}

func (wq *WalQueue) put(req request) error {
	bytes, err := req.marshall()
	if err != nil {
		return err
	}

	wq.walMu.Lock()
	defer wq.walMu.Unlock()
	// This is dealing with strange custom of TidWAL where read index is 0 when there's no data
	if wq.readIndex < 1 {
		wq.readIndex++
	}

	writeErr := wq.wal.Write(wq.writeIndex()+1, bytes)
	if writeErr != nil {
		return writeErr
	}

	return nil
}

func (wq *WalQueue) get() (request, error) {
	wq.walMu.Lock()
	defer wq.walMu.Unlock()
	if wq.readIndex < 1 || wq.readIndex > wq.writeIndex() {
		return nil, wal.ErrNotFound
	}
	bytes, err := wq.wal.Read(wq.readIndex)
	wq.readIndex++
	if err != nil {
		return nil, err
	}
	return wq.unmarshaler(bytes)
}

func (wq *WalQueue) close() error {
	wq.walMu.Lock()
	defer wq.walMu.Unlock()

	return wq.wal.Close()
}

func (wq *WalQueue) sync() error {
	wq.walMu.Lock()
	defer wq.walMu.Unlock()

	// No data
	if wq.readIndex < 1 {
		return nil
	}

	// FIXME: this needs to be confirmed and handled somehow
	// TidWal does not allow truncating single-element queue
	if wq.readIndex > wq.writeIndex() {
		return nil
	}

	err := wq.wal.TruncateFront(wq.readIndex)
	if err != nil {
		return err
	}
	err = wq.wal.Sync()
	if err == nil {
		wq.readIndex, err = wq.wal.FirstIndex()
	}
	return err
}
