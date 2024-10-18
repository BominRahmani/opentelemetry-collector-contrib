// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package anomolyprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/anomolyprocessor"

import (
	"context"
	"math"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics/identity"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/anomolyprocessor/internal/metrics"
)

var _ processor.Metrics = (*Processor)(nil)

type Processor struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	stateLock sync.Mutex

	numberLookup map[identity.Stream][]pmetric.NumberDataPoint

	config *Config

	nextConsumer consumer.Metrics
}

func newProcessor(config *Config, log *zap.Logger, nextConsumer consumer.Metrics) *Processor {
	ctx, cancel := context.WithCancel(context.Background())

	return &Processor{
		ctx:    ctx,
		cancel: cancel,
		logger: log,

		stateLock: sync.Mutex{},

		numberLookup: map[identity.Stream][]pmetric.NumberDataPoint{},

		config: config,

		nextConsumer: nextConsumer,
	}
}

func (p *Processor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *Processor) Shutdown(_ context.Context) error {
	p.cancel()
	return nil
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeSum:
					metricID := identity.OfMetric(identity.OfScope(identity.OfResource(rm.Resource()), sm.Scope()), m)
					p.processNumberDataPoints(m.Sum().DataPoints(), metricID)
					return false
				case pmetric.MetricTypeGauge:
					metricID := identity.OfMetric(identity.OfScope(identity.OfResource(rm.Resource()), sm.Scope()), m)
					p.processNumberDataPoints(m.Gauge().DataPoints(), metricID)
					return false
				default:
					return false
				}
			})
		}
	}

	// After processing, pass the metrics to the next consumer
	return p.nextConsumer.ConsumeMetrics(ctx, md)
}

func (p *Processor) processNumberDataPoints(dataPoints pmetric.NumberDataPointSlice, metricID identity.Metric) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)
		streamID := identity.OfStream(metricID, dp)

		p.numberLookup[streamID] = append(p.numberLookup[streamID], dp)
		if len(p.numberLookup[streamID]) > p.config.DetectionWindow {
			p.numberLookup[streamID] = p.numberLookup[streamID][1:]
		}

		p.detectZScoreAnomaly(streamID)
	}
}

func (p *Processor) detectZScoreAnomaly(streamID identity.Stream) {
	dataPoints := p.numberLookup[streamID]
	if len(dataPoints) < p.config.DetectionWindow {
		return
	}

	mean := calculateMean(dataPoints)
	stdDev := calculateStdDev(dataPoints, mean)
	latestValue := dataPoints[len(dataPoints)-1].DoubleValue()

	isAnomaly := math.Abs(latestValue-mean) > p.config.AnomolyThreshold*stdDev

	if isAnomaly {
		p.logger.Info("Anomaly detected",
			zap.String("metricID", streamID.String()),
			zap.Float64("value", latestValue),
			zap.Float64("mean", mean),
			zap.Float64("stdDev", stdDev))
	}
}

func calculateMean(dataPoints []pmetric.NumberDataPoint) float64 {
	sum := 0.0
	for _, dp := range dataPoints {
		sum += dp.DoubleValue()
	}
	return sum / float64(len(dataPoints))
}

// // σ = √(∑((x−¯x) ( x − x ¯ ))**2 / N )
func calculateStdDev(dataPoints []pmetric.NumberDataPoint, mean float64) float64 {
	sumSquares := 0.0
	for _, dp := range dataPoints {
		diff := dp.DoubleValue() - mean
		sumSquares += diff * diff
	}
	return math.Sqrt(sumSquares / float64(len(dataPoints)))
}

func isAnomaly(value, mean, stdDev, threshold float64) bool {
	return math.Abs(value-mean) > threshold*stdDev
}

func (p *Processor) detectAnomalies(md pmetric.Metrics) {
	// Implement anomaly detection logic here
	// This is a placeholder and needs to be implemented based on your specific requirements
	for streamID, dataPoints := range p.numberLookup {
		if len(dataPoints) < p.config.DetectionWindow {
			continue
		}

		// Simple anomaly detection: check if the latest value is significantly different from the mean
		mean := calculateMean(dataPoints)
		stdDev := calculateStdDev(dataPoints, mean)
		latestValue := dataPoints[len(dataPoints)-1].DoubleValue()

		if isAnomaly(latestValue, mean, stdDev, p.config.AnomolyThreshold) {
			// Mark the metric as anomalous
			// This could involve adding an attribute to the metric or creating a new metric
			p.logger.Info("Anomaly detected",
				zap.String("metricID", streamID.String()),
				zap.Float64("value", latestValue),
				zap.Float64("mean", mean),
				zap.Float64("stdDev", stdDev))
		}
	}
}

func storeDataPoints[DPS metrics.DataPointSlice[DP], DP metrics.DataPoint[DP]](dataPoints DPS, mCloneDataPoints DPS, metricID identity.Metric, dpLookup map[identity.Stream][]DP, detectionWindow int) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)
		streamID := identity.OfStream(metricID, dp)

		dpClone := mCloneDataPoints.AppendEmpty()
		dp.CopyTo(dpClone)

		dpLookup[streamID] = append(dpLookup[streamID], dpClone)
		if len(dpLookup[streamID]) > detectionWindow {
			dpLookup[streamID] = dpLookup[streamID][1:]
		}
	}
}
