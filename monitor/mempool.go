package monitor

import (
	"sync"
	"time"
)

// We use a fixed-size ring buffer over a Map to passively prune old entries
// without needing manual deletion from the caller for dropped transactions.
const maxTxs = 1000

var (
	txsMtx     sync.Mutex
	entryTimes = make(map[string]time.Time, maxTxs)
)

// AddTx records the moment a transaction is received by the node (e.g. at CheckTx).
func AddTx(txHash []byte, t time.Time) {
	txsMtx.Lock()
	defer txsMtx.Unlock()

	hashStr := string(txHash)
	if _, exists := entryTimes[hashStr]; !exists {
		entryTimes[hashStr] = t
	}
}

// GetAndRemoveTxEntry retrieves the transaction arrival time and removes it from memory.
func GetAndRemoveTx(txHash []byte) (time.Time, bool) {
	txsMtx.Lock()
	defer txsMtx.Unlock()

	hashStr := string(txHash)
	val, ok := entryTimes[hashStr]
	if ok {
		delete(entryTimes, hashStr)
	}
	return val, ok
}
