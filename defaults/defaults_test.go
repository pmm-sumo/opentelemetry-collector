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

// Program otelcol is the OpenTelemetry Collector that collects stats
// and traces and exports to a configured backend.
package defaults

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pmm-sumo/opentelemetry-collector/exporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/fileexporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/jaeger/jaegergrpcexporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/jaeger/jaegerthrifthttpexporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/loggingexporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/opencensusexporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/prometheusexporter"
	"github.com/pmm-sumo/opentelemetry-collector/exporter/zipkinexporter"
	"github.com/pmm-sumo/opentelemetry-collector/extension"
	"github.com/pmm-sumo/opentelemetry-collector/extension/healthcheckextension"
	"github.com/pmm-sumo/opentelemetry-collector/extension/pprofextension"
	"github.com/pmm-sumo/opentelemetry-collector/extension/zpagesextension"
	"github.com/pmm-sumo/opentelemetry-collector/processor"
	"github.com/pmm-sumo/opentelemetry-collector/processor/attributesprocessor"
	"github.com/pmm-sumo/opentelemetry-collector/processor/batchprocessor"
	"github.com/pmm-sumo/opentelemetry-collector/processor/memorylimiter"
	"github.com/pmm-sumo/opentelemetry-collector/processor/queuedprocessor"
	"github.com/pmm-sumo/opentelemetry-collector/processor/samplingprocessor/probabilisticsamplerprocessor"
	"github.com/pmm-sumo/opentelemetry-collector/processor/samplingprocessor/tailsamplingprocessor"
	"github.com/pmm-sumo/opentelemetry-collector/processor/spanprocessor"
	"github.com/pmm-sumo/opentelemetry-collector/receiver"
	"github.com/pmm-sumo/opentelemetry-collector/receiver/jaegerreceiver"
	"github.com/pmm-sumo/opentelemetry-collector/receiver/opencensusreceiver"
	"github.com/pmm-sumo/opentelemetry-collector/receiver/prometheusreceiver"
	"github.com/pmm-sumo/opentelemetry-collector/receiver/vmmetricsreceiver"
	"github.com/pmm-sumo/opentelemetry-collector/receiver/zipkinreceiver"
)

func TestDefaultComponents(t *testing.T) {
	expectedExtensions := map[string]extension.Factory{
		"health_check": &healthcheckextension.Factory{},
		"pprof":        &pprofextension.Factory{},
		"zpages":       &zpagesextension.Factory{},
	}
	expectedReceivers := map[string]receiver.Factory{
		"jaeger":     &jaegerreceiver.Factory{},
		"zipkin":     &zipkinreceiver.Factory{},
		"prometheus": &prometheusreceiver.Factory{},
		"opencensus": &opencensusreceiver.Factory{},
		"vmmetrics":  &vmmetricsreceiver.Factory{},
	}
	expectedProcessors := map[string]processor.Factory{
		"attributes":            &attributesprocessor.Factory{},
		"queued_retry":          &queuedprocessor.Factory{},
		"batch":                 &batchprocessor.Factory{},
		"memory_limiter":        &memorylimiter.Factory{},
		"tail_sampling":         &tailsamplingprocessor.Factory{},
		"probabilistic_sampler": &probabilisticsamplerprocessor.Factory{},
		"span":                  &spanprocessor.Factory{},
	}
	expectedExporters := map[string]exporter.Factory{
		"opencensus":         &opencensusexporter.Factory{},
		"prometheus":         &prometheusexporter.Factory{},
		"logging":            &loggingexporter.Factory{},
		"zipkin":             &zipkinexporter.Factory{},
		"jaeger_grpc":        &jaegergrpcexporter.Factory{},
		"jaeger_thrift_http": &jaegerthrifthttpexporter.Factory{},
		"file":               &fileexporter.Factory{},
	}

	factories, err := Components()
	fmt.Println(err)
	assert.Nil(t, err)
	assert.Equal(t, expectedExtensions, factories.Extensions)
	assert.Equal(t, expectedReceivers, factories.Receivers)
	assert.Equal(t, expectedProcessors, factories.Processors)
	assert.Equal(t, expectedExporters, factories.Exporters)
}
