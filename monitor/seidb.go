package monitor

import "fmt"

// LogSeidbFlushSS emits SS flush latency during seidb rootmulti flush().
func LogSeidbFlushSS(version int64, totalMs float64) {
	fmt.Printf("msg=seidb_flush_ss_timing version=%d total_ms=%.3f\n", version, totalMs)
	SeidbFlushSSSeconds.Observe(totalMs / 1000)
}
