package monitor

import "fmt"

// LogSeidbFlushSS emits SS flush latency during seidb rootmulti flush().
func LogSeidbFlushSS(version int64, totalMs float64) {
	fmt.Printf("msg=seidb_flush_ss_timing version=%d total_ms=%.3f\n", version, totalMs)
	SeidbFlushSSSeconds.Observe(totalMs / 1000)
}

// LogSeidbSCApplyChangeset emits SC ApplyChangeSets latency during seidb rootmulti flush().
func LogSeidbSCApplyChangeset(version int64, totalMs float64) {
	fmt.Printf("msg=seidb_sc_apply_changeset_timing version=%d total_ms=%.3f\n", version, totalMs)
	SeidbSCApplyChangesetSeconds.Observe(totalMs / 1000)
}

// LogSeidbComputeHash emits WorkingCommitInfo + CommitInfo.Hash latency in WorkingHash().
func LogSeidbWorkingHashTiming(version int64, totalMs float64) {
	fmt.Printf("msg=seidb_compute_hash_timing version=%d total_ms=%.3f\n", version, totalMs)
	SeidbComputeHashSeconds.Observe(totalMs / 1000)
}
