package state

import (
	"sync"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/types"
)

// orderRecordingBus records the height of every PublishEventNewBlock call in arrival
// order so we can assert the worker preserves strict FIFO (per-height) ordering.
type orderRecordingBus struct {
	types.NopEventBus
	mu     sync.Mutex
	order  []int64
	delay  time.Duration // simulate a slow subscriber to exercise back-pressure
}

func (b *orderRecordingBus) PublishEventNewBlock(d types.EventDataNewBlock) error {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	b.order = append(b.order, d.Block.Height)
	b.mu.Unlock()
	return nil
}

func mkJob(bus types.BlockEventPublisher, height int64) fireEventsJob {
	return fireEventsJob{
		logger:   log.NewNopLogger(),
		eventBus: bus,
		block: &types.Block{
			Header: types.Header{Height: height},
			Data:   types.Data{Txs: nil},
		},
		abciResponse:     &abci.FinalizeBlockResponse{},
		validatorUpdates: nil,
	}
}

// TestAsyncEventFirer_PreservesOrder enqueues many heights through a deliberately slow
// subscriber (forces the buffer to fill -> blocking sends) and asserts events fire in
// strict ascending height order — the core ordering invariant.
func TestAsyncEventFirer_PreservesOrder(t *testing.T) {
	bus := &orderRecordingBus{delay: 1 * time.Millisecond}
	a := newAsyncEventFirer()

	const n = 200
	for h := int64(1); h <= n; h++ {
		a.enqueue(mkJob(bus, h))
	}
	a.stop() // drains all in-flight jobs before returning

	if len(bus.order) != n {
		t.Fatalf("expected %d events fired, got %d (drain lost events)", n, len(bus.order))
	}
	for i, h := range bus.order {
		if h != int64(i+1) {
			t.Fatalf("ordering violated at index %d: got height %d, want %d", i, h, i+1)
		}
	}
}

// TestAsyncEventFirer_DrainOnStop ensures stop() flushes the buffer (no lost events on
// graceful shutdown) and is safe to call more than once.
func TestAsyncEventFirer_DrainOnStop(t *testing.T) {
	bus := &orderRecordingBus{}
	a := newAsyncEventFirer()
	for h := int64(1); h <= 8; h++ {
		a.enqueue(mkJob(bus, h))
	}
	a.stop()
	a.stop() // idempotent — must not panic on double close

	if len(bus.order) != 8 {
		t.Fatalf("expected 8 events after drain, got %d", len(bus.order))
	}
}
