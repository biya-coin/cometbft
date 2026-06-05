package types

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var txHashStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "biyachain",
	Subsystem: "types",
	Name:      "tx_hash_step_seconds",
	Help:      "Sub-step durations inside Txs.Hash.",
	Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
}, []string{"step"})
