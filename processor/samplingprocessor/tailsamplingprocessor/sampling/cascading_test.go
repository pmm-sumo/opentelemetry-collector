// Copyright The OpenTelemetry Authors
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

package sampling

import (
	"sync"
	"testing"
	"time"

	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/consumerdata"
	tsconfig "go.opentelemetry.io/collector/processor/samplingprocessor/tailsamplingprocessor/config"
)

func createSpan(durationMicros int64) tracepb.Span {
	nowTs := time.Now().Unix()
	startTime := timestamp.Timestamp{
		Seconds: nowTs - durationMicros/1000000,
		Nanos:   0,
	}
	endTime := timestamp.Timestamp{
		Seconds: nowTs,
		Nanos:   0,
	}

	return tracepb.Span{
		Attributes: &tracepb.Span_Attributes{
			AttributeMap: map[string]*tracepb.AttributeValue{
				"foo": {Value: &tracepb.AttributeValue_IntValue{IntValue: 55}},
			},
		},
		StartTime: &startTime,
		EndTime:   &endTime,
	}
}

func createTrace(numSpans int, durationMicros int64) *TraceData {
	var spans []*tracepb.Span

	for i := 0; i < numSpans; i++ {
		span := createSpan(durationMicros)
		spans = append(spans, &span)
	}

	return &TraceData{
		Mutex:        sync.Mutex{},
		Decisions:    nil,
		ArrivalTime:  time.Time{},
		DecisionTime: time.Time{},
		SpanCount:    int64(numSpans),
		ReceivedBatches: []consumerdata.TraceData{{
			Spans: spans,
		}},
	}
}

func createCascadingEvaluator() PolicyEvaluator {
	config := tsconfig.PolicyCfg{
		Name:           "test-policy-5",
		Type:           tsconfig.Cascading,
		SpansPerSecond: 1000,
		Rules: []tsconfig.CascadingRuleCfg{
			{
				Name:           "duration",
				SpansPerSecond: 10,
				DurationCfg: &tsconfig.DurationCfg{
					MinDurationMicros: 10000,
				},
			},
			{
				Name:           "everything_else",
				SpansPerSecond: -1,
			},
		},
	}

	cascading, _ := NewCascadingFilter(zap.NewNop(), &config)
	return cascading
}

func TestSampling(t *testing.T) {
	cascading := createCascadingEvaluator()

	decision, _ := cascading.Evaluate([]byte{0}, createTrace(8, 1000000))
	require.Equal(t, Sampled, decision)

	decision, _ = cascading.Evaluate([]byte{1}, createTrace(8, 1000000))
	require.Equal(t, NotSampled, decision)
}

func TestSecondChanceEvaluation(t *testing.T) {
	cascading := createCascadingEvaluator()

	decision, _ := cascading.Evaluate([]byte{0}, createTrace(8, 1000))
	require.Equal(t, SecondChance, decision)

	decision, _ = cascading.Evaluate([]byte{1}, createTrace(8, 1000))
	require.Equal(t, SecondChance, decision)

	// This would never fit anyway
	decision, _ = cascading.Evaluate([]byte{1}, createTrace(8000, 1000))
	require.Equal(t, NotSampled, decision)
}

func TestSecondChanceReevaluation(t *testing.T) {
	cascading := createCascadingEvaluator()

	decision, _ := cascading.EvaluateSecondChance([]byte{1}, createTrace(100, 1000))
	require.Equal(t, Sampled, decision)

	// Too much
	decision, _ = cascading.EvaluateSecondChance([]byte{1}, createTrace(1000, 1000))
	require.Equal(t, NotSampled, decision)

	// Just right
	decision, _ = cascading.EvaluateSecondChance([]byte{1}, createTrace(900, 1000))
	require.Equal(t, Sampled, decision)
}
