package basic

import "github.com/biya-coin/cometbft/libs/metrics"

//go:generate go run ../../../../scripts/metricsgen -struct=Metrics

// Metrics contains metrics exposed by this package.
type Metrics struct {
	// simple metric that tracks the height of the chain.
	Height metrics.Gauge
}
