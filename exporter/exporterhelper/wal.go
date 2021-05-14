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
	"errors"
	"fmt"
	"github.com/jaegertracing/jaeger/pkg/queue"
	"github.com/tidwall/wal"
	"go.uber.org/atomic"
	"path/filepath"
	"sync"
	"time"
)

type WalQueue struct {
	workers      int
	stopWG       sync.WaitGroup
	walMu        sync.Mutex
	wal          *wal.Log
	rWALIndex    uint64
	wWALIndex    uint64
	stopped       *atomic.Uint32
	unmarshaller requestUnmarshaller
}

func newWalQueue(unmarshaller requestUnmarshaller) *WalQueue {
	wq, err := createWalQueue(unmarshaller)
	if err != nil {
		panic(err)
	}
	return wq
}

func createWalQueue(unmarshaller requestUnmarshaller) (*WalQueue, error) {
	_wal, err := wal.Open(filepath.Join("/tmp/wal", "some_name"), &wal.Options{
		SegmentCacheSize: 300,
		NoCopy:           true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	wq := &WalQueue{
		wal:          _wal,
		unmarshaller: unmarshaller,
		stopped: atomic.NewUint32(0),
	}

	wq.rWALIndex, err = _wal.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the first WAL index: %w", err)
	}

	wq.wWALIndex, err = _wal.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the last WAL index: %w", err)
	}

	return wq, nil
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
	wq.stopped.Store(1)
	wq.stopWG.Wait()
	// TODO: sync?
}

func (wq *WalQueue) Size() int {
	wq.walMu.Lock()
	defer wq.walMu.Unlock()
	return int(wq.rWALIndex - wq.wWALIndex)
}

func (wq *WalQueue) put(req request) error {
	bytes, err := req.marshall()
	if err != nil {
		return err
	}

	writeErr := wq.wal.Write(wq.wWALIndex+1, bytes)
	if writeErr != nil {
		return writeErr
	}
	wq.wWALIndex++

	return nil
}

func (wq *WalQueue) get() (request, error) {
	if wq.rWALIndex >= wq.wWALIndex {
		return nil, errors.New("No data")
	}
	bytes, err := wq.wal.Read(wq.rWALIndex)
	wq.rWALIndex++
	if err != nil {
		return nil, err
	}
	return wq.unmarshaller(bytes)
}

func (wq *WalQueue) close() error {
	return wq.wal.Close()
}

func (wq *WalQueue) sync() error {
	wq.wal.TruncateFront(wq.rWALIndex)
	return wq.wal.Sync()
}
