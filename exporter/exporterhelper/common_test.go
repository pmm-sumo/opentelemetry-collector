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
	"errors"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumerhelper"
	"go.opentelemetry.io/collector/consumer/pdata"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
)

var (
	okStatus = trace.Status{Code: trace.StatusCodeOK}

	defaultExporterCfg  = config.NewExporterSettings(config.NewID(typeStr))
	exporterTag, _      = tag.NewKey("exporter")
	defaultExporterTags = []tag.Tag{
		{Key: exporterTag, Value: "test"},
	}
)

func TestErrorToStatus(t *testing.T) {
	require.Equal(t, okStatus, errToStatus(nil))
	require.Equal(t, trace.Status{Code: trace.StatusCodeUnknown, Message: "my_error"}, errToStatus(errors.New("my_error")))
}

func TestBaseExporter(t *testing.T) {
	be := newBaseExporter(&defaultExporterCfg, zap.NewNop(), fromOptions(), nopRequestUnmarshaler())
	require.NoError(t, be.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, be.Shutdown(context.Background()))
}

func TestBaseExporterWithOptions(t *testing.T) {
	want := errors.New("my error")
	be := newBaseExporter(
		&defaultExporterCfg,
		zap.NewNop(),
		fromOptions(
			WithStart(func(ctx context.Context, host component.Host) error { return want }),
			WithShutdown(func(ctx context.Context) error { return want }),
			WithResourceToTelemetryConversion(defaultResourceToTelemetrySettings()),
			WithTimeout(DefaultTimeoutSettings())),
		nopRequestUnmarshaler(),
	)
	require.Equal(t, want, be.Start(context.Background(), componenttest.NewNopHost()))
	require.Equal(t, want, be.Shutdown(context.Background()))
}

func errToStatus(err error) trace.Status {
	if err != nil {
		return trace.Status{Code: trace.StatusCodeUnknown, Message: err.Error()}
	}
	return okStatus
}

func nopTracePusher() consumerhelper.ConsumeTracesFunc {
	return func(ctx context.Context, ld pdata.Traces) error {
		return nil
	}
}

func nopRequestUnmarshaler() requestUnmarshaler {
	return newTraceRequestUnmarshalerFunc(nopTracePusher())
}
