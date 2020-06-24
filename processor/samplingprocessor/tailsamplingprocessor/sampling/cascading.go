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
	"time"

	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/processor/samplingprocessor/tailsamplingprocessor/config"
)

type cascadingPolicy struct {
	logger *zap.Logger

	currentSecond        int64
	maxSpansPerSecond int64
	spansInCurrentSecond int64

	rules []cascadingRuleEvaluation
}

type cascadingRuleEvaluation struct {
	// In fact, this can be only NumericTagFilter, StringTagFilter or AlwaysSample
	evaluator PolicyEvaluator

	currentSecond        int64
	// Set maxSpansPerSecond to zero to make it opportunistic rule (it will fill
	// only if there's space after evaluating other ones)
	maxSpansPerSecond    int64
	spansInCurrentSecond int64
}

func (cp cascadingPolicy) shouldConsider(currSecond int64, trace *TraceData) bool {
	if trace.SpanCount > cp.maxSpansPerSecond {
		// This trace will never fit
		return false
	} else if cp.currentSecond == currSecond && trace.SpanCount > cp.maxSpansPerSecond - cp.spansInCurrentSecond {
		// This trace will not fit in this second, no way
		return false
	} else {
		return true
	}
}

func (cp cascadingPolicy) updateRate(currSecond int64, numSpans int64) Decision {
	if cp.currentSecond != currSecond {
		cp.currentSecond = currSecond
		cp.spansInCurrentSecond = 0
	}

	spansInSecondIfSampled := cp.spansInCurrentSecond + numSpans
	if spansInSecondIfSampled < cp.maxSpansPerSecond {
		cp.spansInCurrentSecond = spansInSecondIfSampled
		return Sampled
	}

	return NotSampled
}


func (cre *cascadingRuleEvaluation) shouldConsider(currSecond int64, cp *cascadingPolicy, trace *TraceData) bool {
	if cre.maxSpansPerSecond < 0 {
		return true // opportunistic rule
	} else if trace.SpanCount > cre.maxSpansPerSecond {
		// This trace will never fit
		return false
	} else if cp.currentSecond == currSecond && trace.SpanCount > cre.maxSpansPerSecond-cre.spansInCurrentSecond {
		// This trace will not fit in this second
		return false
	} else {
		return true
	}
}

func (cre *cascadingRuleEvaluation) updateRate(currSecond int64, trace *TraceData) Decision {
	if cre.currentSecond != currSecond {
		cre.currentSecond = currSecond
		cre.spansInCurrentSecond = 0
	}

	spansInSecondIfSampled := cre.spansInCurrentSecond + trace.SpanCount
	if spansInSecondIfSampled < cre.maxSpansPerSecond {
		cre.spansInCurrentSecond = spansInSecondIfSampled
		return Sampled
	}

	return NotSampled
}

var _ PolicyEvaluator = (*cascadingPolicy)(nil)

// NewNumericAttributeFilter creates a cascading policy evaluator
func NewCascadingFilter(logger *zap.Logger, cfg *config.PolicyCfg) PolicyEvaluator {
	cascadingRules := make([]cascadingRuleEvaluation, 0)

	for _, ruleCfg := range cfg.Rules {
		if ruleCfg.StringAttributeCfg != nil && ruleCfg.NumericAttributeCfg != nil {
			logger.Error("Cascading policy cannot have both string and numeric attributes at the same time")
			// TODO: proper error handling
		}

		evaluator := NewAlwaysSample(logger)

		if ruleCfg.StringAttributeCfg != nil {
			evaluator = NewStringAttributeFilter(logger, ruleCfg.StringAttributeCfg.Key, ruleCfg.StringAttributeCfg.Values)
		} else if ruleCfg.NumericAttributeCfg != nil {
			evaluator = NewNumericAttributeFilter(logger,
				ruleCfg.NumericAttributeCfg.Key,
				ruleCfg.NumericAttributeCfg.MinValue,
				ruleCfg.NumericAttributeCfg.MaxValue)
		}

		cascadingRules = append(cascadingRules, cascadingRuleEvaluation{
			evaluator:         evaluator,
			maxSpansPerSecond: ruleCfg.SpansPerSecond,
		})
	}

	return &cascadingPolicy{
		logger:            logger,
		maxSpansPerSecond: cfg.SpansPerSecond,
		rules:             cascadingRules,
	}
}

// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (cp *cascadingPolicy) OnLateArrivingSpans(earlyDecision Decision, spans []*tracepb.Span) error {
	if earlyDecision == Sampled {
		// Update the current rate, this event means that spans were sampled nevertheless due to previous decision
		cp.updateRate(time.Now().Unix(), int64(len(spans)))
	}
	cp.logger.Debug("Triggering action for late arriving spans in cascading filter")
	return nil
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (cp *cascadingPolicy) Evaluate(traceID []byte, trace *TraceData) (Decision, error) {
	cp.logger.Debug("Evaluating spans in cascading filter")

	currSecond := time.Now().Unix()

	if !cp.shouldConsider(currSecond, trace) {
		return NotSampled, nil
	}

	for _, rule := range cp.rules {
		if rule.maxSpansPerSecond < 0 {
			if ruleDecision, err := rule.evaluator.Evaluate(traceID, trace); err != nil && ruleDecision == Sampled {
				return SecondChance, nil
			}
		} else if rule.shouldConsider(currSecond, cp, trace) {
			if ruleDecision, err := rule.evaluator.Evaluate(traceID, trace); err != nil && ruleDecision == Sampled {
				if rule.updateRate(currSecond, trace) == Sampled {
					policyDecision := cp.updateRate(currSecond, trace.SpanCount)
					return policyDecision, nil
				}
			}
		}
	}

	return NotSampled, nil
}

// EvaluateSecondChance looks if more traces can be fit after initial decisions was made
func (cp *cascadingPolicy) EvaluateSecondChance(traceID []byte, trace *TraceData) (Decision, error) {
	// Lets keep it evaluated for the current batch second
	return cp.updateRate(cp.currentSecond, trace.SpanCount), nil
}

// OnDroppedSpans is called when the trace needs to be dropped, due to memory
// pressure, before the decision_wait time has been reached.
func (cp *cascadingPolicy) OnDroppedSpans(traceID []byte, trace *TraceData) (Decision, error) {
	cp.logger.Debug("Triggering action for dropped spans in cascading filter")
	return NotSampled, nil
}
