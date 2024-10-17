// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package anomolyprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/anomolyprocessor"

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

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

	md                 pmetric.Metrics
	rmLookup           map[identity.Resource]pmetric.ResourceMetrics
	smLookup           map[identity.Scope]pmetric.ScopeMetrics
	mLookup            map[identity.Metric]pmetric.Metric
	numberLookup       map[identity.Stream][]pmetric.NumberDataPoint
	histogramLookup    map[identity.Stream][]pmetric.HistogramDataPoint
	expHistogramLookup map[identity.Stream][]pmetric.ExponentialHistogramDataPoint
	summaryLookup      map[identity.Stream][]pmetric.SummaryDataPoint

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

		md:                 pmetric.NewMetrics(),
		rmLookup:           map[identity.Resource]pmetric.ResourceMetrics{},
		smLookup:           map[identity.Scope]pmetric.ScopeMetrics{},
		mLookup:            map[identity.Metric]pmetric.Metric{},
		numberLookup:       map[identity.Stream][]pmetric.NumberDataPoint{},
		histogramLookup:    map[identity.Stream][]pmetric.HistogramDataPoint{},
		expHistogramLookup: map[identity.Stream][]pmetric.ExponentialHistogramDataPoint{},
		summaryLookup:      map[identity.Stream][]pmetric.SummaryDataPoint{},

		config: config,

		nextConsumer: nextConsumer,
	}
}

func (p *Processor) Start(_ context.Context, _ component.Host) error {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			default:
				p.exportMetrics()
			}
		}
	}()

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
	var errs error

	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeSummary:
					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					storeDataPoints(m.Summary().DataPoints(), mClone.Summary().DataPoints(), metricID, p.summaryLookup, p.config.DetectionWindow)
				case pmetric.MetricTypeGauge:
					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					storeDataPoints(m.Gauge().DataPoints(), mClone.Gauge().DataPoints(), metricID, p.numberLookup, p.config.DetectionWindow)
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					if !sum.IsMonotonic() || sum.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						return false
					}
					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					storeDataPoints(m.Sum().DataPoints(), mClone.Sum().DataPoints(), metricID, p.numberLookup, p.config.DetectionWindow)
				case pmetric.MetricTypeHistogram:
					histogram := m.Histogram()
					if histogram.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						return false
					}
					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					cloneHistogram := mClone.Histogram()
					storeDataPoints(m.Histogram().DataPoints(), cloneHistogram.DataPoints(), metricID, p.histogramLookup, p.config.DetectionWindow)
				case pmetric.MetricTypeExponentialHistogram:
					expHistogram := m.ExponentialHistogram()
					if expHistogram.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						return false
					}
					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					cloneExpHistogram := mClone.ExponentialHistogram()
					storeDataPoints(m.ExponentialHistogram().DataPoints(), cloneExpHistogram.DataPoints(), metricID, p.expHistogramLookup, p.config.DetectionWindow)
				default:
					errs = errors.Join(errs, fmt.Errorf("invalid MetricType %d", m.Type()))
					return false
				}
				return false
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	p.detectZScoreAnomoly(md)

	if err := p.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}

func (p *Processor) detectZScoreAnomoly(md pmetric.Metrics) {
	for streamID, dataPoints := range p.numberLookup {
		if len(dataPoints) < p.config.DetectionWindow {
			continue
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

}

func calculateMean(dataPoints []pmetric.NumberDataPoint) float64 {
	sum := 0.0
	for _, dp := range dataPoints {
		sum += dp.DoubleValue()
	}
	return sum / float64(len(dataPoints))
}


// σ = √(∑((x−¯x) ( x − x ¯ ))**2 / N )
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
	// Implement similar logic for other metric types
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

func (p *Processor) exportMetrics() {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	// Clear all the lookup references
	clear(p.rmLookup)
	clear(p.smLookup)
	clear(p.mLookup)
	clear(p.numberLookup)
	clear(p.histogramLookup)
	clear(p.expHistogramLookup)
	clear(p.summaryLookup)
}

func (p *Processor) getOrCloneMetric(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) (pmetric.Metric, identity.Metric) {
	// Find the ResourceMetrics
	resID := identity.OfResource(rm.Resource())
	rmClone, ok := p.rmLookup[resID]
	if !ok {
		// We need to clone it *without* the ScopeMetricsSlice data
		rmClone = p.md.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(rmClone.Resource())
		rmClone.SetSchemaUrl(rm.SchemaUrl())
		p.rmLookup[resID] = rmClone
	}

	// Find the ScopeMetrics
	scopeID := identity.OfScope(resID, sm.Scope())
	smClone, ok := p.smLookup[scopeID]
	if !ok {
		// We need to clone it *without* the MetricSlice data
		smClone = rmClone.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(smClone.Scope())
		smClone.SetSchemaUrl(sm.SchemaUrl())
		p.smLookup[scopeID] = smClone
	}

	// Find the Metric
	metricID := identity.OfMetric(scopeID, m)
	mClone, ok := p.mLookup[metricID]
	if !ok {
		// We need to clone it *without* the datapoint data
		mClone = smClone.Metrics().AppendEmpty()
		mClone.SetName(m.Name())
		mClone.SetDescription(m.Description())
		mClone.SetUnit(m.Unit())

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			mClone.SetEmptyGauge()
		case pmetric.MetricTypeSummary:
			mClone.SetEmptySummary()
		case pmetric.MetricTypeSum:
			src := m.Sum()

			dest := mClone.SetEmptySum()
			dest.SetAggregationTemporality(src.AggregationTemporality())
			dest.SetIsMonotonic(src.IsMonotonic())
		case pmetric.MetricTypeHistogram:
			src := m.Histogram()

			dest := mClone.SetEmptyHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		case pmetric.MetricTypeExponentialHistogram:
			src := m.ExponentialHistogram()

			dest := mClone.SetEmptyExponentialHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		}

		p.mLookup[metricID] = mClone
	}

	return mClone, metricID
}
