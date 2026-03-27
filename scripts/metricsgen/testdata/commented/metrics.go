package commented

import "github.com/biya-coin/cometbft/libs/metrics"

//go:generate go run ../../../../scripts/metricsgen -struct=Metrics

type Metrics struct {
	// Height of the chain.
	// We expect multi-line comments to parse correctly.
	Field metrics.Gauge
}
