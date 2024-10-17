// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package anomolyprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/anomolyprocessor"

import (

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

type Config struct {
	DetectionWindow  int             `mapstructure:"detection_window"`
	Detector         AnomolyDetector `mapstructure:"detector"`
	AnomolyThreshold float64         `mapstructure:"anomoly_threshold"`
}

type AnomolyDetector struct {
	ZScore     bool `mapstructure:"z_score"`
	Percentile bool `mapstructure:"percentile"`
	DBScan     bool `mapstructure:"dbscan"`
}

// Validate checks whether the input configuration has all of the required fields for the processor.
// An error is returned if there are any invalid inputs.
func (config *Config) Validate() error {

	return nil
}
