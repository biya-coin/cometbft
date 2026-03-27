package psql

import (
	"github.com/biya-coin/cometbft/state/indexer"
	"github.com/biya-coin/cometbft/state/txindex"
)

var (
	_ indexer.BlockIndexer = BackportBlockIndexer{}
	_ txindex.TxIndexer    = BackportTxIndexer{}
)
