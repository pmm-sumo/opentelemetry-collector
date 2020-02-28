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

// Package defaults composes the default set of components used by the otel service
package defaults

import (
	"github.com/pmm-sumo/opentelemetry-collector/config"
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
	"github.com/pmm-sumo/opentelemetry-collector/oterr"
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

// Components returns the default set of components used by the
// OpenTelemetry collector.
func Components() (
	config.Factories,
	error,
) {
	errs := []error{}

	extensions, err := extension.Build(
		&healthcheckextension.Factory{},
		&pprofextension.Factory{},
		&zpagesextension.Factory{},
	)
	if err != nil {
		errs = append(errs, err)
	}

	receivers, err := receiver.Build(
		&jaegerreceiver.Factory{},
		&zipkinreceiver.Factory{},
		&prometheusreceiver.Factory{},
		&opencensusreceiver.Factory{},
		&vmmetricsreceiver.Factory{},
	)
	if err != nil {
		errs = append(errs, err)
	}

	exporters, err := exporter.Build(
		&opencensusexporter.Factory{},
		&prometheusexporter.Factory{},
		&loggingexporter.Factory{},
		&zipkinexporter.Factory{},
		&jaegergrpcexporter.Factory{},
		&jaegerthrifthttpexporter.Factory{},
		&fileexporter.Factory{},
	)
	if err != nil {
		errs = append(errs, err)
	}

	processors, err := processor.Build(
		&attributesprocessor.Factory{},
		&queuedprocessor.Factory{},
		&batchprocessor.Factory{},
		&memorylimiter.Factory{},
		&tailsamplingprocessor.Factory{},
		&probabilisticsamplerprocessor.Factory{},
		&spanprocessor.Factory{},
	)
	if err != nil {
		errs = append(errs, err)
	}

	factories := config.Factories{
		Extensions: extensions,
		Receivers:  receivers,
		Processors: processors,
		Exporters:  exporters,
	}

	return factories, oterr.CombineErrors(errs)
}
