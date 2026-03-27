package versionservice

import (
	"context"

	pbsvc "github.com/biya-coin/cometbft/api/cometbft/services/version/v1"
	"github.com/biya-coin/cometbft/version"
)

type versionServiceServer struct{}

// New creates a new CometBFT version service server.
func New() pbsvc.VersionServiceServer {
	return &versionServiceServer{}
}

// GetVersion implements v1.VersionServiceServer.
func (*versionServiceServer) GetVersion(context.Context, *pbsvc.GetVersionRequest) (*pbsvc.GetVersionResponse, error) {
	return &pbsvc.GetVersionResponse{
		Node:  version.CMTSemVer,
		Abci:  version.ABCIVersion,
		P2P:   version.P2PProtocol,
		Block: version.BlockProtocol,
	}, nil
}
