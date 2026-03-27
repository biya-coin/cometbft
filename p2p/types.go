package p2p

import (
	"github.com/cosmos/gogoproto/proto"

	tmp2p "github.com/biya-coin/cometbft/api/cometbft/p2p/v1"
	"github.com/biya-coin/cometbft/p2p/conn"
	"github.com/biya-coin/cometbft/types"
)

type (
	ChannelDescriptor = conn.ChannelDescriptor
	ConnectionStatus  = conn.ConnectionStatus
)

// Envelope contains a message with sender routing info.
type Envelope struct {
	Src       Peer          // sender (empty if outbound)
	Message   proto.Message // message payload
	ChannelID byte
}

var (
	_ types.Wrapper = &tmp2p.PexRequest{}
	_ types.Wrapper = &tmp2p.PexAddrs{}
)
