package mempool

type ProxyMempool struct {
	Mempool
}

var _ Mempool = (*ProxyMempool)(nil)

func (m *ProxyMempool) SetMempool(mp Mempool) {
	m.Mempool = mp
}

// TODO: add assertions for all methods

// ProxyMempool implements TxBroadcastStream interface
func (mp *ProxyMempool) GetNextTx() <-chan *mempoolTx {
	panic("implement me")
}
