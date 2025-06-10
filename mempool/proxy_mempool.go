package mempool

// ProxyMempool is a wrapper around a Mempool and a TxBroadcastStream.
// It allows setting the underlying Mempool and TxBroadcastStream dynamically.
type ProxyMempool struct {
	Mempool
	TxBroadcastStream
}

var _ Mempool = (*ProxyMempool)(nil)
var _ TxBroadcastStream = (*ProxyMempool)(nil)

func (m *ProxyMempool) SetMempool(mp Mempool) {
	m.Mempool = mp
}

func (m *ProxyMempool) SetTxBroadcastStream(stream TxBroadcastStream) {
	m.TxBroadcastStream = stream
}
