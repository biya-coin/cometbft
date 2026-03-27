package statesync

import (
	ssproto "github.com/biya-coin/cometbft/api/cometbft/statesync/v1"
	"github.com/biya-coin/cometbft/types"
)

var (
	_ types.Wrapper = &ssproto.ChunkRequest{}
	_ types.Wrapper = &ssproto.ChunkResponse{}
	_ types.Wrapper = &ssproto.SnapshotsRequest{}
	_ types.Wrapper = &ssproto.SnapshotsResponse{}
)
