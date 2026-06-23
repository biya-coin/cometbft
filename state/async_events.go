package state

import (
	"os"
	"sync"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/types"
)

// async_events: BIYA perf lever (env-gated by BIYA_ASYNC_FIRE_EVENTS=1).
//
// fireEvents runs at the very end of applyBlock, AFTER Commit + store.Save. It is
// pure observability — it only publishes to the eventBus (websocket / KV tx-indexer
// subscribers) and touches NO app state and NO AppHash. On the saturated 4-val bench
// it costs ~76-83ms on the consensus-critical path, post-decision and OUTSIDE the
// OptimisticExecution voting-window overlap, so moving it off-path is a genuine
// net block-interval reduction (it does not rebound into FinalizeBlock the way
// propagation levers do).
//
// Design invariants:
//   - ORDERING: a single FIFO worker goroutine consumes jobs, so per-height event
//     order is preserved exactly as in the synchronous path.
//   - BACKPRESSURE: the channel is bounded and the producer (consensus thread) BLOCKS
//     when it is full. A slow subscriber therefore throttles consensus to at most the
//     synchronous behavior — never dropping events, never growing memory unbounded,
//     and never reordering (a degrade-to-inline-fire would emit a later height ahead
//     of queued earlier ones, so we must block instead).
//   - DETERMINISM: events are independent of consensus state; the 4-val AppHash is
//     unaffected by when they fire (the determinism gate confirms this).
//
// SEMANTIC CAVEAT: with async enabled, height N's events fire AFTER applyBlock returns,
// so consensus may have already advanced to height N+1 by the time a subscriber receives
// N's EventNewBlock. Upstream fires them before applyBlock returns. This is acceptable for
// observability subscribers (websocket, KV indexer) but a subscriber that correlates RPC
// consensus height with event arrival must tolerate the event lagging the height.
//
// Default (env unset) leaves blockExec.asyncFire nil → byte-identical to upstream.

const asyncFireEventsBuf = 8

// asyncFireEventsEnabled reports whether the env gate is on. Read once at construction.
func asyncFireEventsEnabled() bool {
	return os.Getenv("BIYA_ASYNC_FIRE_EVENTS") == "1"
}

type fireEventsJob struct {
	logger           log.Logger
	eventBus         types.BlockEventPublisher
	block            *types.Block
	blockID          types.BlockID
	abciResponse     *abci.FinalizeBlockResponse
	validatorUpdates []*types.Validator
}

// asyncEventFirer moves fireEvents onto a single ordered worker goroutine.
type asyncEventFirer struct {
	ch       chan fireEventsJob
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newAsyncEventFirer() *asyncEventFirer {
	a := &asyncEventFirer{ch: make(chan fireEventsJob, asyncFireEventsBuf)}
	a.wg.Add(1)
	go a.run()
	return a
}

func (a *asyncEventFirer) run() {
	defer a.wg.Done()
	for job := range a.ch {
		fireEvents(job.logger, job.eventBus, job.block, job.blockID, job.abciResponse, job.validatorUpdates)
	}
}

// enqueue submits a fireEvents job to the worker. It BLOCKS when the buffer is full
// (back-pressure ≈ synchronous behavior) to preserve strict per-height ordering.
func (a *asyncEventFirer) enqueue(job fireEventsJob) {
	a.ch <- job
}

// stop closes the queue and drains any in-flight jobs so a graceful shutdown does not
// lose the last block's events. Safe to call multiple times.
func (a *asyncEventFirer) stop() {
	a.stopOnce.Do(func() { close(a.ch) })
	a.wg.Wait()
}
