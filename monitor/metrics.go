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

	PrepareLaneSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "prepare_lane_seconds",
		Help:      "Time spent in PrepareLane per lane (handler / get_info / update).",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
	}, []string{"lane", "step"}) // step: handler / get_info / update
)

// ── FinalizeBlock / internalFinalizeBlock（秒） ──────────────────────────────
// 对应 msg=app_finalize_block / msg=app_internal_finalize_block
var (
	FinalizeBlockSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "finalize",
		Name:      "block_duration_seconds",
		Help:      "Total FinalizeBlock duration (ABCI call, app side).",
		Buckets:   []float64{0.5, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15},
	})

	WorkingHashSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "finalize",
		Name:      "working_hash_seconds",
		Help:      "Time to compute WorkingHash after FinalizeBlock.",
		Buckets:   []float64{0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4},
	})

	InternalFinalizeBlockSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "finalize",
		Name:      "internal_step_seconds",
		Help:      "Sub-step durations inside internalFinalizeBlock.",
		Buckets:   []float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8},
	}, []string{"step"}) // begin_block / execute_txs / end_block / total
)

// ── ExecuteTxs 子步骤（秒） ──────────────────────────────────────────────────
// 对应 msg=execute_txs_substep
var (
	ExecuteTxsStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "execute_txs",
		Name:      "step_seconds",
		Help:      "Per-block time for ante / msgs / post handler across all txs.",
		Buckets:   []float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8},
	}, []string{"step"}) // ante / msgs / post
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

// ── BaseApp / rootmulti Commit（秒） ─────────────────────────────────────────
// 对应 msg=baseapp_commit_timing / msg=rootmulti_commit_timing
var (
	BaseAppCommitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "baseapp_commit",
		Name:      "step_seconds",
		Help:      "BaseApp Commit sub-step durations.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
	}, []string{"step"}) // total / cms_commit

	RootmultiCommitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "rootmulti_commit",
		Name:      "step_seconds",
		Help:      "rootmulti.Commit sub-step durations.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
	}, []string{"step"}) // total / version_calc / commit_stores / flush_metadata / cleanup_removed / prune

	RootmultiStoreCommitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "rootmulti_commit",
		Name:      "store_seconds",
		Help:      "Per-store commit duration inside rootmulti.Commit.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5},
	}, []string{"store"})
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
