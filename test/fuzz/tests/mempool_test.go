//go:build gofuzz || go1.20

package tests

import (
	"context"
	"testing"

	abciclient "github.com/biya-coin/cometbft/abci/client"
	"github.com/biya-coin/cometbft/abci/example/kvstore"
	"github.com/biya-coin/cometbft/config"
	cmtsync "github.com/biya-coin/cometbft/libs/sync"
	mempl "github.com/biya-coin/cometbft/mempool"
	"github.com/biya-coin/cometbft/proxy"
)

func FuzzMempool(f *testing.F) {
	app := kvstore.NewInMemoryApplication()
	mtx := new(cmtsync.Mutex)
	conn := abciclient.NewLocalClient(mtx, app)
	err := conn.Start()
	if err != nil {
		panic(err)
	}

	cfg := config.DefaultMempoolConfig()
	cfg.Broadcast = false

	resp, err := app.Info(context.Background(), proxy.InfoRequest)
	if err != nil {
		panic(err)
	}
	lanesInfo, err := mempl.BuildLanesInfo(resp.LanePriorities, resp.DefaultLane)
	if err != nil {
		panic(err)
	}
	mp := mempl.NewCListMempool(cfg, conn, lanesInfo, 0)

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = mp.CheckTx(data, "")
	})
}
