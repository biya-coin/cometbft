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
	senders *sync.Map
}

// NewMempoolTx creates a new mempoolTx using the builder pattern
func NewMempoolTx(tx types.Tx) MempoolTx {
	return NewMempoolTxBuilder().
		WithTx(tx).
		Build()
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

func (memTx *mempoolTx) AddSender(peerId uint16) bool {
	return memTx.addSender(peerId)
}

func (memTx *mempoolTx) Tx() types.Tx {
	return memTx.tx
}
func (memTx *mempoolTx) GasWanted() int64 {
	return atomic.LoadInt64(&memTx.gasWanted)
}

// MempoolTxBuilder is a builder for creating mempoolTx instances
type MempoolTxBuilder struct {
	height    int64
	gasWanted int64
	tx        types.Tx
	senders   []uint16
}

// NewMempoolTxBuilder creates a new builder for mempoolTx
func NewMempoolTxBuilder() *MempoolTxBuilder {
	return &MempoolTxBuilder{
		senders: make([]uint16, 0),
	}
}

// WithHeight sets the height for the mempoolTx
func (b *MempoolTxBuilder) WithHeight(height int64) *MempoolTxBuilder {
	b.height = height
	return b
}

// WithGasWanted sets the gas wanted for the mempoolTx
func (b *MempoolTxBuilder) WithGasWanted(gasWanted int64) *MempoolTxBuilder {
	b.gasWanted = gasWanted
	return b
}

// WithTx sets the transaction for the mempoolTx
func (b *MempoolTxBuilder) WithTx(tx types.Tx) *MempoolTxBuilder {
	b.tx = tx
	return b
}

func (b *MempoolTxBuilder) WithSender(sender uint16) *MempoolTxBuilder {
	b.senders = append(b.senders, sender)
	return b
}

// Build creates the final mempoolTx instance
func (b *MempoolTxBuilder) Build() MempoolTx {
	senders := &sync.Map{}
	for _, sender := range b.senders {
		senders.Store(sender, true)
	}

	memTx := &mempoolTx{
		height:    b.height,
		gasWanted: b.gasWanted,
		tx:        b.tx,
		senders:   senders,
	}
	return memTx
}
