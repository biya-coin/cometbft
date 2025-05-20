package mempool

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

// TODO: add assertions for all methods
