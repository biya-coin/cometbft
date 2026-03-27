package core

import (
	ctypes "github.com/biya-coin/cometbft/rpc/core/types"
	rpctypes "github.com/biya-coin/cometbft/rpc/jsonrpc/types"
)

// UnsafeFlushMempool removes all transactions from the mempool.
func (env *Environment) UnsafeFlushMempool(*rpctypes.Context) (*ctypes.ResultUnsafeFlushMempool, error) {
	env.Mempool.Flush()
	return &ctypes.ResultUnsafeFlushMempool{}, nil
}
