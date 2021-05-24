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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/extension/storage"
	"go.opentelemetry.io/collector/extension/storage/storagetest"
)

func createStorageExtension(path string) storage.Extension {
	return storagetest.NewTestExtension(nil, path)
}

func createTestQueue(extension storage.Extension) *WALQueue {
	logger, _ := zap.NewDevelopment()

	client, err := extension.GetClient(context.Background(), component.KindReceiver, config.ComponentID{})
	if err != nil {
		panic(err)
	}

	return newWALQueue(context.Background(), "foo", logger, client, newTraceRequestUnmarshalerFunc(nopTracePusher()))
}

func createTestWALStorage(extension storage.Extension) walStorage {
	logger, _ := zap.NewDevelopment()

	client, err := extension.GetClient(context.Background(), component.KindReceiver, config.ComponentID{})
	if err != nil {
		panic(err)
	}

	return newWALContiguousStorage(context.Background(), "foo", logger, client, newTraceRequestUnmarshalerFunc(nopTracePusher()))
}

func createTemporaryDirectory() string {
	directory, err := ioutil.TempDir("", "wal-test")
	if err != nil {
		panic(err)
	}
	return directory
}

func TestWal_RepeatPutCloseReadClose(t *testing.T) {
	path := createTemporaryDirectory()
	defer os.RemoveAll(path)

	traces := newTraces(1, 10)
	req := newTracesRequest(context.Background(), traces, nopTracePusher())

	for i := 0; i < 10; i++ {
		ext := createStorageExtension(path)
		wcs := createTestWALStorage(ext)
		require.Equal(t, 0, wcs.size())
		err := wcs.put(context.Background(), req)
		require.NoError(t, err)
		err = ext.Shutdown(context.Background())
		require.NoError(t, err)

		ext = createStorageExtension(path)
		wcs = createTestWALStorage(ext)
		require.Equal(t, 1, wcs.size())
		readReq, err := wcs.get(context.Background())
		require.NoError(t, err)
		require.Equal(t, 0, wcs.size())
		require.Equal(t, req.(*tracesRequest).td, readReq.(*tracesRequest).td)
		err = ext.Shutdown(context.Background())
		require.NoError(t, err)
	}

	// No more items
	ext := createStorageExtension(path)
	wq := createTestQueue(ext)
	require.Equal(t, 0, wq.Size())
	ext.Shutdown(context.Background())
}

func TestWal_ConsumersProducers(t *testing.T) {
	cases := []struct {
		numMessagesProduced int
		numConsumers        int
	}{
		{
			numMessagesProduced: 1,
			numConsumers:        1,
		},
		{
			numMessagesProduced: 100,
			numConsumers:        1,
		},
		{
			numMessagesProduced: 100,
			numConsumers:        3,
		},
		{
			numMessagesProduced: 1,
			numConsumers:        100,
		},
		{
			numMessagesProduced: 100,
			numConsumers:        100,
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("#messages: %d #consumers: %d", c.numMessagesProduced, c.numConsumers), func(t *testing.T) {
			path := createTemporaryDirectory()

			traces := newTraces(1, 10)
			req := newTracesRequest(context.Background(), traces, nopTracePusher())

			ext := createStorageExtension(path)
			wq := createTestQueue(ext)
			defer ext.Shutdown(context.Background())
			defer os.RemoveAll(path)

			numMessagesConsumed := int32(0)
			wq.StartConsumers(c.numConsumers, func(item interface{}) {
				if item != nil {
					atomic.AddInt32(&numMessagesConsumed, 1)
				}
			})

			for i := 0; i < c.numMessagesProduced; i++ {
				wq.Produce(req)
			}

			require.Eventually(t, func() bool {
				return c.numMessagesProduced == int(atomic.LoadInt32(&numMessagesConsumed))
			}, 1*time.Second, 10*time.Millisecond)
		})
	}
}

func BenchmarkWal_1Trace10Spans(b *testing.B) {
	path := createTemporaryDirectory()
	defer os.RemoveAll(path)
	ext := createStorageExtension(path)
	wcs := createTestWALStorage(ext)

	traces := newTraces(1, 10)
	req := newTracesRequest(context.Background(), traces, nopTracePusher())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := wcs.put(context.Background(), req)
		require.NoError(b, err)
	}

	for i := 0; i < b.N; i++ {
		req, err := wcs.get(context.Background())
		require.NoError(b, err)
		require.NotNil(b, req)
	}
	ext.Shutdown(context.Background())
}

func newTraces(numTraces int, numSpans int) pdata.Traces {
	traces := pdata.NewTraces()
	batch := traces.ResourceSpans().AppendEmpty()
	batch.Resource().Attributes().InsertString("resource-attr", "some-resource")
	batch.Resource().Attributes().InsertInt("num-traces", int64(numTraces))
	batch.Resource().Attributes().InsertInt("num-spans", int64(numSpans))

	for i := 0; i < numTraces; i++ {
		traceID := pdata.NewTraceID([16]byte{1, 2, 3, byte(i)})
		ils := batch.InstrumentationLibrarySpans().AppendEmpty()
		for j := 0; j < numSpans; j++ {
			span := ils.Spans().AppendEmpty()
			span.SetTraceID(traceID)
			span.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, byte(j)}))
			span.SetName("should-not-be-changed")
			span.Attributes().InsertInt("int-attribute", int64(j))
			span.Attributes().InsertString("str-attribute-1", "foobar")
			span.Attributes().InsertString("str-attribute-2", "fdslafjasdk12312312jkl")
			span.Attributes().InsertString("str-attribute-3", "AbcDefGeKKjkfdsafasdfsdasdf")
			span.Attributes().InsertString("str-attribute-4", "xxxxxx")
			span.Attributes().InsertString("str-attribute-5", "abcdef")
		}
	}

	return traces
}
