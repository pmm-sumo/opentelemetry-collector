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

package loggingexporter

import (
	"context"
	"fmt"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector/config/configmodels"
	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/exporter"
	"github.com/open-telemetry/opentelemetry-collector/exporter/exporterhelper"
)

// NewTraceExporter creates an exporter.TraceExporter that just drops the
// received data and logs debugging messages.
func NewTraceExporter(config configmodels.Exporter, logger *zap.Logger) (exporter.TraceExporter, error) {
	typeLog := zap.String("type", config.Type())
	nameLog := zap.String("name", config.Name())
	return exporterhelper.NewTraceExporter(
		config,
		func(ctx context.Context, td consumerdata.TraceData) (int, error) {
			logger.Info("TraceExporter", typeLog, nameLog,
				zap.Int("#spans", len(td.Spans)))

			if td.Resource != nil {
				logger.Info("TraceExporter Resource",
					zap.String("type", td.Resource.Type),
					zap.Int("#resourceLabels", len(td.Resource.Labels)))

				logMap(logger, "Resource labels", &td.Resource.Labels)
			}

			if td.Node != nil {
				logger.Debug("Node service name: "+td.Node.ServiceInfo.Name)
				logMap(logger, "Node attributes", &td.Node.Attributes)
			}

			for i, span := range td.Spans {
				logger.Debug("Span #"+string(i))
				if span == nil {
					logger.Debug("* Empty span")
					continue
				}

				logAttr(logger, "Trace ID", string(span.TraceId))
				logAttr(logger, "ID", string(span.SpanId))
				logAttr(logger, "Parent ID", string(span.ParentSpanId))
				logAttr(logger, "Name", span.Name.Value)
				logAttr(logger, "Kind", span.Kind.String())
				logAttr(logger, "Start time", span.StartTime.String())
				logAttr(logger, "End time", span.EndTime.String())
				if span.Status != nil {
					logAttr(logger, "Status code", string(span.Status.Code))
					logAttr(logger, "Status message", span.Status.Message)
				}

				if span.Attributes != nil {
					logAttr(logger, "Span attributes", "")
					for attr, value := range span.Attributes.AttributeMap {
						logger.Debug(fmt.Sprintf("         -> %s: %s", attr, value.String()))
					}
				}
			}

			// TODO: Add ability to record the received data
			return 0, nil
		},
		exporterhelper.WithTracing(true),
		exporterhelper.WithMetrics(true),
		exporterhelper.WithShutdown(logger.Sync),
	)
}

func logAttr(logger *zap.Logger, label string, value string) {
	logger.Debug(fmt.Sprintf("    %-12s: %s", label, value))
}

func logMap(logger *zap.Logger, label string, data *map[string]string) {
	if data == nil {
		return
	}

	logger.Debug(label+":")
	for label, value := range *data {
		logger.Debug("     -> "+label+": "+value)
	}
}

// NewMetricsExporter creates an exporter.MetricsExporter that just drops the
// received data and logs debugging messages.
func NewMetricsExporter(config configmodels.Exporter, logger *zap.Logger) (exporter.MetricsExporter, error) {
	typeLog := zap.String("type", config.Type())
	nameLog := zap.String("name", config.Name())
	return exporterhelper.NewMetricsExporter(
		config,
		func(ctx context.Context, md consumerdata.MetricsData) (int, error) {
			logger.Info("MetricsExporter", typeLog, nameLog, zap.Int("#metrics", len(md.Metrics)))
			// TODO: Add ability to record the received data
			return 0, nil
		},
		exporterhelper.WithTracing(true),
		exporterhelper.WithMetrics(true),
		exporterhelper.WithShutdown(logger.Sync),
	)
}
