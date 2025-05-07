package mempool

import (
	"sync"
	"sync/atomic"

	"github.com/cometbft/cometbft/types"
)

var _ MempoolTx = (*mempoolTx)(nil)

// mempoolTx is an entry in the mempool
type mempoolTx struct {
	height    int64    // height that this tx had been validated in
	gasWanted int64    // amount of gas this tx states it will require
	tx        types.Tx // validated by the application

	// ids of peers who've sent us this tx (as a map for quick lookups).
	// senders: PeerID -> bool
	senders sync.Map
}

func NewMempoolTx(height int64, gasWanted int64, tx types.Tx, senderId uint16) MempoolTx {
	memTx := &mempoolTx{
		height:    height,
		gasWanted: gasWanted,
		tx:        tx,
	}

	memTx.addSender(senderId)

	return memTx
}

// Height returns the height for this transaction
func (memTx *mempoolTx) Height() int64 {
	return atomic.LoadInt64(&memTx.height)
}

func (memTx *mempoolTx) isSender(peerID uint16) bool {
	_, ok := memTx.senders.Load(peerID)
	return ok
}

func (memTx *mempoolTx) addSender(senderID uint16) bool {
	_, added := memTx.senders.LoadOrStore(senderID, true)
	return added
}

func (memTx *mempoolTx) IsSender(peerId uint16) bool {
	return memTx.isSender(peerId)
}

func (memTx *mempoolTx) Tx() types.Tx {
	return memTx.tx
}
func (memTx *mempoolTx) GasWanted() int64 {
	return atomic.LoadInt64(&memTx.gasWanted)
}
