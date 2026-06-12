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

	DoPrevoteDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consensus",
		Name:      "do_prevote_duration_seconds",
		Help:      "Duration of doPrevote during the propose phase.",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	DoPrevoteValidateBlockDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consensus",
		Name:      "do_prevote_validate_block_duration_seconds",
		Help:      "Duration of ValidateBlock inside doPrevote.",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	EnterPrecommitDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consensus",
		Name:      "enter_precommit_duration_seconds",
		Help:      "Duration of enterPrecommit during the prevote phase.",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})
)

// ── PrepareProposal / DecideProposal（秒） ───────────────────────────────────
// 对应 msg=decide_proposal_timing / msg=create_proposal_block_timing /
// msg=prepare_lane_timing / msg=lane_sim_timing / msg=sdk_prepare_timing
var (
	DecideProposalSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "decide_duration_seconds",
		Help:      "Total wall-clock time for defaultDecideProposal (leader only).",
		Buckets:   []float64{0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 5},
	})

	DecideProposalStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "decide_step_seconds",
		Help:      "Sub-step durations inside defaultDecideProposal.",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"step"})

	CreateProposalBlockSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "create_block_seconds",
		Help:      "Time spent in consensus State.createProposalBlock.",
		Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 5},
	})

	CreateProposalBlockStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "create_block_step_seconds",
		Help:      "Sub-step durations inside consensus State.createProposalBlock.",
		Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 5},
	}, []string{"step"})
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

// ── SeiDB flush SS（秒） ─────────────────────────────────────────────────────
// 对应 msg=seidb_flush_ss_timing
var (
	SeidbWorkingHashStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "seidb_working_hash",
		Name:      "step_seconds",
		Help:      "Duration of each SeiDB WorkingHash sub-step.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5},
	}, []string{"step"})

	SeidbFlushSSSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "seidb_flush",
		Name:      "ss_seconds",
		Help:      "Duration of SS flush inside seidb rootmulti flush().",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
	})
)

// ── SeiDB SC apply changeset（秒） ───────────────────────────────────────────
// 对应 msg=seidb_sc_apply_changeset_timing
var (
	SeidbSCApplyChangesetSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "seidb_sc",
		Name:      "apply_changeset_seconds",
		Help:      "Duration of SC ApplyChangeSets inside seidb rootmulti flush().",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5},
	})
)

// ── SeiDB compute hash（秒） ─────────────────────────────────────────────────
// 对应 msg=seidb_compute_hash_timing
var (
	SeidbComputeHashSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "seidb_hash",
		Name:      "compute_seconds",
		Help:      "Duration of WorkingCommitInfo + CommitInfo.Hash in seidb WorkingHash().",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5},
	})
)

// ── BaseApp workingHash 子步骤（秒） ─────────────────────────────────────────
// 对应 msg=working_hash_ms_write_timing / msg=working_hash_cms_working_hash_timing
var (
	WorkingHashMsWriteSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "working_hash",
		Name:      "ms_write_seconds",
		Help:      "Duration of finalizeBlockState.ms.Write() inside BaseApp.workingHash().",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5},
	})

	WorkingHashCmsWorkingHashSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "working_hash",
		Name:      "cms_working_hash_seconds",
		Help:      "Duration of cms.WorkingHash() inside BaseApp.workingHash().",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5},
	})
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
