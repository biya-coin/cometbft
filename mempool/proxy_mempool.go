package mempool

import "github.com/cometbft/cometbft/proxy"

type ProxyMempool struct {
	Mempool
	TxBroadcastStream
	appConnMempool proxy.AppConnMempool
}

type CustomMempool interface {
	Mempool
	SetAppConnMempool(appConnMempool proxy.AppConnMempool)
}

func NewProxyMempool(appConnMempool proxy.AppConnMempool) *ProxyMempool {
	return &ProxyMempool{
		appConnMempool: appConnMempool,
	}
}

var _ Mempool = (*ProxyMempool)(nil)
var _ TxBroadcastStream = (*ProxyMempool)(nil)

func (m *ProxyMempool) SetMempool(mp CustomMempool) {
	mp.SetAppConnMempool(m.appConnMempool)
	m.Mempool = mp
}

func (m *ProxyMempool) SetTxBroadcastStream(stream TxBroadcastStream) {
	m.TxBroadcastStream = stream
}

// TODO: add assertions for all methods
