// Package loki 提供供 Loki 日志采集的结构化打印函数，同时管理 TX 生命周期计时存储。
// 这是与 Prometheus metrics 体系完全独立的监测体系，
// 通过 fmt.Printf 输出 logfmt 格式日志，由 Promtail 采集后存入 Loki。
package loki

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/biya-coin/cometbft/types"
)

// ─── TX 生命周期时间存储（原 txtracer 包逻辑）────────────────────────────────

const maxTrackedTxs = 1000

var (
	tsMu    sync.Mutex
	tsStore = make(map[string]time.Time, maxTrackedTxs) // txHashHex -> mempoolEntryTime
	tsOrder []string                                    // insertion order for bounded eviction
)

// OnTxAddedToMempool 记录 TX 成功加入 mempool 的时刻。
// 由 clist_mempool.go 中 addTx() 在 CheckTx 成功后调用。
func OnTxAddedToMempool(txHash []byte) {
	hashStr := hex.EncodeToString(txHash)

	tsMu.Lock()
	defer tsMu.Unlock()

	if _, exists := tsStore[hashStr]; exists {
		return
	}
	// Evict oldest when at capacity.
	if len(tsStore) >= maxTrackedTxs {
		evict := tsOrder[0]
		tsOrder = tsOrder[1:]
		delete(tsStore, evict)
	}
	tsStore[hashStr] = time.Now()
	tsOrder = append(tsOrder, hashStr)
}

// popMempoolTime 从存储中取出并删除指定 txHash 的 mempool 入队时刻。
// 内部使用，供 LogTxLifecycles 调用。
func popMempoolTime(txHash []byte) (time.Time, bool) {
	hashStr := hex.EncodeToString(txHash)

	tsMu.Lock()
	defer tsMu.Unlock()

	mempoolAt, ok := tsStore[hashStr]
	if ok {
		delete(tsStore, hashStr)
		for i, h := range tsOrder {
			if h == hashStr {
				tsOrder = append(tsOrder[:i], tsOrder[i+1:]...)
				break
			}
		}
	}
	return mempoolAt, ok
}

// ─── lastRound0At：出块间隔计时 ───────────────────────────────────────────────

// lastRound0At 记录上一个高度调度 Round0 的本地时刻，用于计算真实出块间隔。
// 在 LogConsensusTiming 调用时更新为当前时刻。
var lastRound0At time.Time

// LastRound0At 返回上一个高度调度 Round0 的时刻。
func LastRound0At() time.Time {
	return lastRound0At
}

// ─── Loki 日志函数 ────────────────────────────────────────────────────────────

// LogCommitTiming 输出每块一行的 commit 子阶段耗时日志，供 Loki 采集。
// 封装 FinalizeBlock 和 PostFinalizeBlock 两个阶段的耗时（对应 Prometheus [8-3] 和 [8-4]）。
func LogCommitTiming(height int64, finalizeBlockUs int64, d1, d2, d3, d4, d5, d6 int64) {
	fmt.Printf("msg=commit_subevent_timing height=%d finalize_block_us=%d d1=%d d2=%d d3=%d d4=%d d5=%d d6=%d\n",
		height, finalizeBlockUs, d1, d2, d3, d4, d5, d6)
}

// LogTxLifecycles 为本块中每笔交易打印生命周期耗时日志，供 Loki 采集。
// 从内部存储中取出 mempool 入队时刻，计算各阶段耗时后输出一行日志。
// 必须在 LogConsensusTiming 之前调用，以使用上一高度的 lastRound0At 作为本块共识起始时刻。
func LogTxLifecycles(height int64, txs types.Txs, accums map[string]int64, blockIntervalUs int64) {
	round0At := lastRound0At // 快照，避免并发修改
	for _, tx := range txs {
		mempoolAt, ok := popMempoolTime(tx.Hash())
		if !ok {
			// Transaction not tracked (e.g. arrived before node start or from state-sync).
			continue
		}
		// mempool_wait: from mempool entry to this block's consensus start.
		mempoolWaitUs := round0At.Sub(mempoolAt).Microseconds()
		if mempoolWaitUs < 0 {
			mempoolWaitUs = 0
		}
		totalUs := time.Since(mempoolAt).Microseconds()
		fmt.Printf(
			"msg=tx_lifecycle tx_hash=%x block_height=%d"+
				" mempool_wait_us=%d block_interval_us=%d"+
				" new_height_us=%d new_round_us=%d propose_us=%d"+
				" prevote_us=%d prevote_wait_us=%d"+
				" precommit_us=%d precommit_wait_us=%d"+
				" commit_us=%d total_us=%d\n",
			tx.Hash(), height,
			mempoolWaitUs, blockIntervalUs,
			accums["NewHeight"], accums["NewRound"], accums["Propose"],
			accums["Prevote"], accums["PrevoteWait"],
			accums["Precommit"], accums["PrecommitWait"],
			accums["Commit"], totalUs,
		)
	}
}

// LogConsensusTiming 输出每块一行的共识各阶段耗时日志，供 Loki 采集，
// 同时将 lastRound0At 更新为当前时刻（即本高度 Round0 的调度时刻）。
//
//   - accums: 各共识阶段累积耗时（μs），由调用方通过 metrics.ResetStepAccums() 获取
//   - validatorCount: 当前验证节点总数，写入日志供 Grafana Loki 面板展示（无需 Prometheus）
func LogConsensusTiming(height int64, accums map[string]int64, txs types.Txs, validatorCount int) {
	var blockIntervalUs int64
	if height > 1 && !lastRound0At.IsZero() {
		// 用本节点上一次调度 Round0 的时刻计算真实出块间隔
		// 这反映节点真实处理节奏，而非区块头时间戳之差
		blockIntervalUs = time.Since(lastRound0At).Microseconds()
	}
	fmt.Printf("msg=consensus_timing height=%d block_interval_us=%d"+
		" new_height_us=%d new_round_us=%d propose_us=%d"+
		" prevote_us=%d prevote_wait_us=%d"+
		" precommit_us=%d precommit_wait_us=%d commit_us=%d tx_count=%d validator_count=%d\n",
		height, blockIntervalUs,
		accums["NewHeight"], accums["NewRound"], accums["Propose"],
		accums["Prevote"], accums["PrevoteWait"],
		accums["Precommit"], accums["PrecommitWait"], accums["Commit"],
		int64(len(txs)), validatorCount,
	)
	// 调用时记录本高度 Round0 的调度时刻，供下一个高度计算出块间隔
	lastRound0At = time.Now()

	LogTxLifecycles(height, txs, accums, blockIntervalUs)
}
