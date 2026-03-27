package mempool

import (
	memprotos "github.com/biya-coin/cometbft/api/cometbft/mempool/v2"
	"github.com/biya-coin/cometbft/types"
)

var (
	_ types.Wrapper   = &memprotos.Txs{}
	_ types.Wrapper   = &memprotos.HaveTx{}
	_ types.Wrapper   = &memprotos.ResetRoute{}
	_ types.Unwrapper = &memprotos.Message{}
)
