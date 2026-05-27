// Package monitor – Prometheus metrics definitions for CometBFT custom instrumentation.
//
// These histograms/gauges mirror every field already exposed via Loki (fmt.Printf),
// so both pipelines stay in sync. The buckets are chosen to match typical
// block-time distributions observed in production (ms or seconds as noted).
package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "biyachain"

// ── 共识阶段耗时（秒） ────────────────────────────────────────────────────────
// 对应 Loki msg=consensus 的各字段
var (
	ConsensusPhaseSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consensus",
		Name:      "phase_duration_seconds",
		Help:      "Duration of each consensus phase per block.",
		Buckets:   prometheus.DefBuckets, // 0.005~10s
	}, []string{"phase"}) // new_height / new_round / propose / prevote / precommit / commit / total

	TxsPerBlock = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consensus",
		Name:      "txs_per_block",
		Help:      "Number of transactions committed per block.",
		Buckets:   []float64{10, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000},
	})
)

// ── PrepareProposal / DecideProposal（秒） ───────────────────────────────────
// 对应 msg=decide_proposal_timing / msg=create_proposal_block_timing /
//
//	msg=local_client_prepare_proposal_timing / msg=prepare_lane_timing /
//	msg=lane_sim_timing / msg=sdk_prepare_timing
var (
	DecideProposalSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "decide_duration_seconds",
		Help:      "Total wall-clock time for defaultDecideProposal (leader only).",
		Buckets:   []float64{0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 5},
	})

	ReapSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "reap_duration_seconds",
		Help:      "Time to reap transactions from mempool (ReapMaxBytesMaxGas).",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})

	PrepareProposalLockWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "prepare_lock_wait_seconds",
		Help:      "Time waiting for ABCI mutex before PrepareProposal.",
		Buckets:   []float64{0.0001, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2},
	})

	PrepareProposalExecSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "prepare_exec_seconds",
		Help:      "Execution time of PrepareProposal ABCI call.",
		Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 5},
	})

)

// ── ApplyBlock 子步骤（秒） ──────────────────────────────────────────────────
// 对应 msg=apply_block_substep
var (
	ApplyBlockStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "apply_block",
		Name:      "step_seconds",
		Help:      "Sub-step durations inside ApplyVerifiedBlock.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 6, 8},
	}, []string{"step"}) // ab1_finalize / ab2_save_resp / ab3_update_state / ab4_commit / ab5_evpool / ab6_store_save / ab7_fire_events
)

// ── Commit 子步骤（秒） ──────────────────────────────────────────────────────
// 对应 msg=block_exec_commit_substep
var (
	CommitStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "commit",
		Name:      "step_seconds",
		Help:      "Sub-step durations inside Commit().",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2},
	}, []string{"step"}) // mempool_preupdate / mempool_lock / flush_app_conn / abci_commit
)


// ── Mempool reap lock wait（秒） ──────────────────────────────────────────
// 对应 msg=reap_lock_timing
var (
	ReapLockWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "mempool",
		Name:      "reap_lock_wait_seconds",
		Help:      "Time waiting for updateMtx RLock in ReapMaxBytesMaxGas.",
		Buckets:   []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25},
	})
)
