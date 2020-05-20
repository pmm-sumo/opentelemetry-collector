// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batchprocessor

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/data/testdata"
	"math"
	"testing"
)

func testSplitCase(t *testing.T, resourceCount int, spansPerResource int, limitPerBatch int) {
	generatedSpans := 0
	traces := pdata.NewTraces()
	for resourceNum := 0; resourceNum < resourceCount; resourceNum++ {
		td := testdata.GenerateTraceDataManySpansSameResource(spansPerResource)
		spans := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans()
		for resourceNumSpanIndex := 0; resourceNumSpanIndex < spansPerResource; resourceNumSpanIndex++ {
			spans.At(resourceNumSpanIndex).SetName(getTestSpanName(resourceNum, resourceNumSpanIndex))
		}
		generatedSpans += spansPerResource
		td.ResourceSpans().MoveAndAppendTo(traces.ResourceSpans())
	}

	splitTraces := SplitItems(traces, limitPerBatch)

	expectedTraceSplits := int(math.Max(1, math.Ceil(float64(generatedSpans)/float64(limitPerBatch))))
	require.Equal(t, expectedTraceSplits, len(splitTraces))

	// Iterate all spans

	resourceNum := 0
	resourceNumSpanIndex := 0
	traceIndex := 0
	resourceIndex := 0
	librarySpansIndex := 0
	spanIndex := 0
	matchedSpans := 0

	for resourceNum < resourceCount {
		td := splitTraces[traceIndex]
		rss := td.ResourceSpans().At(resourceIndex)
		ils := rss.InstrumentationLibrarySpans().At(librarySpansIndex)
		if ils.Spans().Len() > 0 {
			span := ils.Spans().At(spanIndex)
			require.EqualValues(t, getTestSpanName(resourceNum, resourceNumSpanIndex), span.Name())
			spanIndex++
			matchedSpans++
		}

		if spanIndex == ils.Spans().Len() {
			spanIndex = 0
			librarySpansIndex++
		}

		if librarySpansIndex == rss.InstrumentationLibrarySpans().Len() {
			librarySpansIndex = 0
			resourceIndex++
		}

		if resourceIndex == td.ResourceSpans().Len() {
			resourceIndex = 0
			traceIndex++
		}

		resourceNumSpanIndex++
		if resourceNumSpanIndex >= spansPerResource {
			resourceNumSpanIndex = 0
			resourceNum++
		}
	}

	require.Equal(t, generatedSpans, matchedSpans)
}

func TestSpltting(t *testing.T) {
	resourceCounts := []int{0, 1, 9, 10}
	spanPerResourceCounts := []int{0, 1, 9, 10, 11, 100, 500}
	limits := []int{1, 5, 9, 11, 50, 99, 100, 101, 102, 499, 500, 501, 999, 1000, 1001}

	for _, resourceCount := range resourceCounts {
		for _, spanPerResourceCount := range spanPerResourceCounts {
			for _, limit := range limits {
				fmt.Printf("Resources: %4d | Spans per resource: %4d | Limit: %4d\n", resourceCount, spanPerResourceCount, limit)
				testSplitCase(t, resourceCount, spanPerResourceCount, limit)
			}
		}
	}
}
