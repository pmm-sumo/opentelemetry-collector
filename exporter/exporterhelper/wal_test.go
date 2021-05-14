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
	"github.com/nsqio/go-diskqueue"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/pdata"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestWal_PutIntoQueue(t *testing.T) {

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
		for j := 0;  j < numSpans; j++ {
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

func BenchmarkWal_1Trace10Spans(b *testing.B) {
	b.StopTimer()

	wq, err := createWalQueue(newTraceRequestUnmarshallerFunc(nopTracePusher()))
	assert.NoError(b, err)

	traces := newTraces(1, 10)
	req := newTracesRequest(context.Background(), traces, nopTracePusher())
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := wq.put(req)
		assert.NoError(b, err)
	}

	for i := 0; i < b.N; i++ {
		req, err := wq.get()
		assert.NoError(b, err)
		assert.NotNil(b, req)
	}

	b.StopTimer()
	wq.close()
}

func BenchmarkDiskQueue_1Trace10Spans(b *testing.B) {
	b.StopTimer()

	dqName := "test_disk_queue_roll"
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	traces := newTraces(1, 10)
	msg, _ := traces.ToOtlpProtoBytes()
	ml := int64(len(msg))

	dq := diskqueue.New(dqName, tmpDir, 100*(ml+4), int32(ml), 1<<16, 25000, 5*time.Second, func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		println(f, args)
	})
	defer dq.Close()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := dq.Put(msg)
		assert.NoError(b, err)
	}

	for i := 0; i < b.N; i++ {
		<-dq.ReadChan()
	}

	b.StopTimer()
}
