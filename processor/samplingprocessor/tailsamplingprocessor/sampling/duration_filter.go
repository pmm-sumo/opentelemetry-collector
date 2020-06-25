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
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"
)

type durationFilter struct {
	minDurationMicros int64
	logger            *zap.Logger
}

var _ PolicyEvaluator = (*durationFilter)(nil)

// NewDurationFilter creates a policy evaluator that samples all traces with
// the duration exceeding min duration
func NewDurationFilter(logger *zap.Logger, minDurationMicros int64) PolicyEvaluator {
	return &durationFilter{
		minDurationMicros: minDurationMicros,
		logger:            logger,
	}
}

// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (df *durationFilter) OnLateArrivingSpans(earlyDecision Decision, spans []*tracepb.Span) error {
	return nil
}

// EvaluateSecondChance looks at the trace again and if it can/cannot be fit, returns a SamplingDecision
func (df *durationFilter) EvaluateSecondChance(traceID []byte, trace *TraceData) (Decision, error) {
	return NotSampled, nil
}

func tsToMicros(ts *timestamp.Timestamp) int64 {
	return ts.Seconds*1000000 + int64(ts.Nanos/1000)
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (df *durationFilter) Evaluate(traceID []byte, trace *TraceData) (Decision, error) {
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()

	minStartTime := int64(0)
	maxEndTime := int64(0)
	for _, batch := range batches {
		for _, span := range batch.Spans {
			if span == nil {
				continue
			}
			startTs := tsToMicros(span.StartTime)
			endTs := tsToMicros(span.EndTime)

			if minStartTime == 0 {
				minStartTime = startTs
				maxEndTime = endTs
			} else {
				if startTs < minStartTime {
					minStartTime = startTs
				}
				if endTs > maxEndTime {
					maxEndTime = endTs
				}
			}
		}
	}

	// Sanity check first
	if maxEndTime > minStartTime && maxEndTime-minStartTime >= df.minDurationMicros {
		return Sampled, nil
	}

	return NotSampled, nil
}

// OnDroppedSpans is called when the trace needs to be dropped, due to memory
// pressure, before the decision_wait time has been reached.
func (df *durationFilter) OnDroppedSpans(traceID []byte, trace *TraceData) (Decision, error) {
	return NotSampled, nil
}
