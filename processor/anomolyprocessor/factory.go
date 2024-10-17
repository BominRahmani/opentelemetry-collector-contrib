// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package anomolyprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/anomolyprocessor"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

)

// NewFactory returns a new factory for the Metrics Generation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("anomoly"),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Interval: 180 * time.Second, // default to three minutes, however the longer the interval the more accurate the anomoly detection
	}
}

func createMetricsProcessor(_ context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Metrics) (processor.Metrics, error) {
	processorConfig, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}

	return newProcessor(processorConfig, set.Logger, nextConsumer), nil
}
