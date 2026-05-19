// Package monitor provides a centralized Loki-compatible structured logging
// monitor for CometBFT consensus and mempool subsystems.
//
// Consensus-phase durations are fed directly from the existing Prometheus
// Metrics.MarkStep() call. All phase durations are accumulated silently
// and emitted as a single log line when the block is committed.
//
// LogQL example:
//
//	{job="cometbft"} |= "consensus_block" | logfmt | unwrap total_ms
package monitor

import (
	"fmt"
	"strings"
	"time"

	"github.com/cometbft/cometbft/types"
)

// ConsensusMonitor accumulates per-phase timing provided by Prometheus Metrics
// and flushes a single Loki-compatible log line per committed block.
type ConsensusMonitor struct {
	blockStart      time.Time
	newHeightMs     float64
	newRoundMs      float64
	proposeMs       float64
	prevoteMs       float64
	prevoteWaitMs   float64
	precommitMs     float64
	precommitWaitMs float64
	commitMs        float64
}

// NewConsensusMonitor creates a ConsensusMonitor.
func NewConsensusMonitor() *ConsensusMonitor {
	return &ConsensusMonitor{
		blockStart: time.Now(), // Initialize for the very first block
	}
}

// RecordStep receives the duration of one step from Prometheus' MarkStep.
func (m *ConsensusMonitor) RecordStep(stepName string, durationSeconds float64) {
	ms := durationSeconds * 1000
	switch strings.ToLower(stepName) {
	case "newheight":
		m.newHeightMs += ms
	case "newround":
		m.newRoundMs += ms
	case "propose":
		m.proposeMs += ms
	case "prevote":
		m.prevoteMs += ms
	case "prevotewait":
		m.prevoteWaitMs += ms
	case "precommit":
		m.precommitMs += ms
	case "precommitwait":
		m.precommitWaitMs += ms
	case "commit":
		m.commitMs += ms
	}
}

// FlushBlock emits one log line containing all collected phase durations plus
// the total wall-clock block time, and also prints the wait time for all included
// transactions. Call at the end of finalizeCommit.
func (m *ConsensusMonitor) FlushBlock(height int64, txs types.Txs) {
	if m.blockStart.IsZero() {
		return
	}
	totalMs := float64(time.Since(m.blockStart).Nanoseconds()) / 1e6
	numTxs := len(txs)

	// Print individual transaction wait times
	// for _, tx := range txs {
	// 	enterTime, ok := GetAndRemoveTx(tx.Hash())
	// 	if ok && !enterTime.IsZero() {
	// 		waitMs := float64(m.blockStart.Sub(enterTime).Nanoseconds()) / 1e6
	// 		if waitMs < 0 {
	// 			waitMs = 0
	// 		}
	// 		fmt.Printf("msg=mempool_tx_wait height=%d hash=%X wait_ms=%.2f\n",
	// 			height, tx.Hash(), waitMs)
	// 	}
	// }

	// Explicitly print each phase duration so they are clearly visible in the code and logs.
	fmt.Printf("msg=txs height=%d txs=%d\n", height, numTxs)
	fmt.Printf("msg=consensus height=%d new_height=%.2f new_round=%.2f propose=%.2f prevote=%.2f prevote_wait=%.2f precommit=%.2f precommit_wait=%.2f commit=%.2f total=%.2f\n",
		height,
		m.newHeightMs, m.newRoundMs, m.proposeMs, m.prevoteMs, m.prevoteWaitMs, m.precommitMs, m.precommitWaitMs, m.commitMs, totalMs)

	// Initialize for the next block cycle by reusing the constructor.
	*m = *NewConsensusMonitor()
}

// LogApplyBlockSubstep emits the 7 sub-step latencies for ApplyVerifiedBlock.
func LogApplyBlockSubstep(height int64, t0, t1, t2, t3, t4, t5, t6, t7 time.Time) {
	fmt.Printf("msg=apply_block_substep height=%d ab1_finalize_ms=%.3f ab2_save_resp_ms=%.3f ab3_update_state_ms=%.3f ab4_commit_ms=%.3f ab5_evpool_ms=%.3f ab6_store_save_ms=%.3f ab7_fire_events_ms=%.3f\n",
		height,
		float64(t1.Sub(t0).Nanoseconds())/1e6,
		float64(t2.Sub(t1).Nanoseconds())/1e6,
		float64(t3.Sub(t2).Nanoseconds())/1e6,
		float64(t4.Sub(t3).Nanoseconds())/1e6,
		float64(t5.Sub(t4).Nanoseconds())/1e6,
		float64(t6.Sub(t5).Nanoseconds())/1e6,
		float64(t7.Sub(t6).Nanoseconds())/1e6,
	)
}

// LogCommitSubstep emits the ApplyVerifiedBlock latency for the commit phase.
func LogCommitSubstep(height int64, t0, t3 time.Time) {
	fmt.Printf("msg=commit_substep height=%d d3_apply_block_ms=%.3f\n",
		height,
		float64(t3.Sub(t0).Nanoseconds())/1e6,
	)
}

// LogBlockExecCommitSubstep emits the 4 synchronous sub-step latencies inside Commit():
// t0=PreUpdate, t1=Lock, t2=FlushAppConn, t3=ABCI Commit
func LogBlockExecCommitSubstep(height int64, t0, t1, t2, t3 time.Duration) {
	fmt.Printf("msg=block_exec_commit_substep height=%d mempool_preupdate_ms=%.3f mempool_lock_ms=%.3f flush_app_conn_ms=%.3f abci_commit_ms=%.3f\n",
		height,
		float64(t0.Nanoseconds())/1e6,
		float64(t1.Nanoseconds())/1e6,
		float64(t2.Nanoseconds())/1e6,
		float64(t3.Nanoseconds())/1e6,
	)
}
