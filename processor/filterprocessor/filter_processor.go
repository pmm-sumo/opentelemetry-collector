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

package filterprocessor

import (
	"context"

	"go.opentelemetry.io/collector/internal/processor/filterconfig"
	"go.opentelemetry.io/collector/internal/processor/filterspan"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/processor/filtermetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

type filterProcessor struct {
	cfg            *Config
	includeMetrics filtermetric.Matcher
	excludeMetrics filtermetric.Matcher
	includeSpans   filterspan.Matcher
	excludeSpans   filterspan.Matcher
	logger         *zap.Logger
}

func newFilterMetricProcessor(logger *zap.Logger, cfg *Config) (*filterProcessor, error) {
	inc, err := createMetricsMatcher(cfg.Metrics.Include)
	if err != nil {
		return nil, err
	}

	exc, err := createMetricsMatcher(cfg.Metrics.Exclude)
	if err != nil {
		return nil, err
	}

	includeMatchType := ""
	var includeExpressions []string
	var includeMetricNames []string
	if cfg.Metrics.Include != nil {
		includeMatchType = string(cfg.Metrics.Include.MatchType)
		includeExpressions = cfg.Metrics.Include.Expressions
		includeMetricNames = cfg.Metrics.Include.MetricNames
	}

	excludeMatchType := ""
	var excludeExpressions []string
	var excludeMetricNames []string
	if cfg.Metrics.Exclude != nil {
		excludeMatchType = string(cfg.Metrics.Exclude.MatchType)
		excludeExpressions = cfg.Metrics.Exclude.Expressions
		excludeMetricNames = cfg.Metrics.Exclude.MetricNames
	}

	logger.Info(
		"Metric filter configured",
		zap.String("includeMetrics match_type", includeMatchType),
		zap.Strings("includeMetrics expressions", includeExpressions),
		zap.Strings("includeMetrics metric names", includeMetricNames),
		zap.String("excludeMetrics match_type", excludeMatchType),
		zap.Strings("excludeMetrics expressions", excludeExpressions),
		zap.Strings("excludeMetrics metric names", excludeMetricNames),
	)

	return &filterProcessor{
		cfg:            cfg,
		includeMetrics: inc,
		excludeMetrics: exc,
		logger:         logger,
	}, nil
}

func newFilterSpanProcessor(logger *zap.Logger, cfg *Config) (*filterProcessor, error) {
	inc, err := createSpansMatcher(cfg.Spans.Include)
	if err != nil {
		return nil, err
	}

	exc, err := createSpansMatcher(cfg.Spans.Exclude)
	if err != nil {
		return nil, err
	}

	includeMatchType := ""
	var includeServices []string
	var includeSpanNames []string
	if cfg.Spans.Include != nil {
		includeMatchType = string(cfg.Spans.Include.MatchType)
		includeServices = cfg.Spans.Include.Services
		includeSpanNames = cfg.Spans.Include.SpanNames
	}

	excludeMatchType := ""
	var excludeServices []string
	var excludeSpanNames []string
	if cfg.Spans.Exclude != nil {
		excludeMatchType = string(cfg.Spans.Exclude.MatchType)
		excludeServices = cfg.Spans.Exclude.Services
		excludeSpanNames = cfg.Spans.Exclude.SpanNames
	}

	logger.Info(
		"Span filter configured",
		zap.String("includeSpans match_type", includeMatchType),
		zap.Strings("includeSpans services", includeServices),
		zap.Strings("includeSpans span names", includeSpanNames),
		zap.String("excludeSpans match_type", excludeMatchType),
		zap.Strings("excludeSpans services", excludeServices),
		zap.Strings("excludeSpans span names", excludeSpanNames),
	)

	return &filterProcessor{
		cfg:          cfg,
		includeSpans: inc,
		excludeSpans: exc,
		logger:       logger,
	}, nil
}

func createSpansMatcher(mp *filterconfig.MatchProperties) (filterspan.Matcher, error) {
	if mp == nil {
		return nil, nil
	}
	return filterspan.NewMatcher(mp)
}

func createMetricsMatcher(mp *filtermetric.MatchProperties) (filtermetric.Matcher, error) {
	// Nothing specified in configuration
	if mp == nil {
		return nil, nil
	}
	return filtermetric.NewMatcher(mp)
}

// ProcessMetrics filters the given metrics based off the filterProcessor's filters.
func (fmp *filterProcessor) ProcessMetrics(_ context.Context, pdm pdata.Metrics) (pdata.Metrics, error) {
	rms := pdm.ResourceMetrics()
	idx := newMetricIndex()
	for i := 0; i < rms.Len(); i++ {
		ilms := rms.At(i).InstrumentationLibraryMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ms := ilms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				keep, err := fmp.shouldKeepMetric(ms.At(k))
				if err != nil {
					fmp.logger.Error("shouldKeepMetric failed", zap.Error(err))
					// don't `continue`, keep the metric if there's an error
				}
				if keep {
					idx.add(i, j, k)
				}
			}
		}
	}
	if idx.isEmpty() {
		return pdm, processorhelper.ErrSkipProcessingData
	}
	return idx.extract(pdm), nil
}

// ProcessTraces filters the given spans based off the filterProcessor's filters.
func (fmp *filterProcessor) ProcessTraces(_ context.Context, pdt pdata.Traces) (pdata.Traces, error) {
	rs := pdt.ResourceSpans()
	for i := 0; i < rs.Len(); i++ {
		rss := rs.At(i)
		resource := rss.Resource()
		ils := rss.InstrumentationLibrarySpans()

		for j := 0; j < ils.Len(); j++ {
			ilss := ils.At(j)
			library := ilss.InstrumentationLibrary()
			inputSpans := pdata.NewSpanSlice()
			ilss.Spans().MoveAndAppendTo(inputSpans)
			for k := 0; k < inputSpans.Len(); k++ {
				span := inputSpans.At(k)
				if fmp.shouldKeepSpan(span, resource, library) {
					ilss.Spans().Append(span)
				}
			}
		}
	}

	return pdt, nil
}

func (fmp *filterProcessor) shouldKeepMetric(metric pdata.Metric) (bool, error) {
	if fmp.includeMetrics != nil {
		matches, err := fmp.includeMetrics.MatchMetric(metric)
		if err != nil {
			// default to keep if there's an error
			return true, err
		}
		if !matches {
			return false, nil
		}
	}

	if fmp.excludeMetrics != nil {
		matches, err := fmp.excludeMetrics.MatchMetric(metric)
		if err != nil {
			return true, err
		}
		if matches {
			return false, nil
		}
	}

	return true, nil
}

func (fmp *filterProcessor) shouldKeepSpan(span pdata.Span, resource pdata.Resource, library pdata.InstrumentationLibrary) bool {
	if fmp.includeSpans != nil {
		matches := fmp.includeSpans.MatchSpan(span, resource, library)
		if !matches {
			return false
		}
	}

	if fmp.excludeSpans != nil {
		matches := fmp.excludeSpans.MatchSpan(span, resource, library)
		if matches {
			return false
		}
	}

	return true
}
