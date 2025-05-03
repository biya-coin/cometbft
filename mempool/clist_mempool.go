package mempool

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/clist"
	"github.com/cometbft/cometbft/libs/log"
	cmtmath "github.com/cometbft/cometbft/libs/math"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

// CListMempool is an ordered in-memory pool for transactions before they are
// proposed in a consensus round. Transaction validity is checked using the
// CheckTx abci message before the transaction is added to the pool. The
// mempool uses a concurrent list structure for storing transactions that can
// be efficiently accessed by multiple concurrent readers.
type CListMempool struct {
	height   atomic.Int64 // the last block Update()'d to
	txsBytes atomic.Int64 // total size of mempool, in bytes

	// notify listeners (ie. consensus) when txs are available
	notifiedTxsAvailable atomic.Bool
	txsAvailable         chan struct{} // fires once for each height, when the mempool is not empty

	config *config.MempoolConfig

	// Exclusive mutex for Update method to prevent concurrent execution of
	// CheckTx or ReapMaxBytesMaxGas(ReapMaxTxs) methods.
	updateMtx cmtsync.RWMutex
	preCheck  PreCheckFunc
	postCheck PostCheckFunc

	txs          *clist.CList // concurrent linked-list of good txs
	proxyAppConn proxy.AppConnMempool

	// Keeps track of the rechecking process.
	recheck *recheck

	// Map for quick access to txs to record sender in CheckTx.
	// txsMap: txKey -> CElement
	txsMap sync.Map

	// Keep a cache of already-seen txs.
	// This reduces the pressure on the proxyApp.
	cache TxCache

	logger  log.Logger
	metrics *Metrics
}

var _ Mempool = &CListMempool{}

// CListMempoolOption sets an optional parameter on the mempool.
type CListMempoolOption func(*CListMempool)

// NewCListMempool returns a new mempool with the given configuration and
// connection to an application.
func NewCListMempool(
	cfg *config.MempoolConfig,
	proxyAppConn proxy.AppConnMempool,
	height int64,
	options ...CListMempoolOption,
) *CListMempool {
	mp := &CListMempool{
		config:       cfg,
		proxyAppConn: proxyAppConn,
		txs:          clist.New(),
		recheck:      newRecheck(),
		logger:       log.NewNopLogger(),
		metrics:      NopMetrics(),
	}
	mp.height.Store(height)

	if cfg.CacheSize > 0 {
		mp.cache = NewLRUTxCache(cfg.CacheSize)
	} else {
		mp.cache = NopTxCache{}
	}

	for _, option := range options {
		option(mp)
	}

	return mp
}

func (mem *CListMempool) getCElement(txKey types.TxKey) (*clist.CElement, bool) {
	if e, ok := mem.txsMap.Load(txKey); ok {
		return e.(*clist.CElement), true
	}
	return nil, false
}

func (mem *CListMempool) getMemTx(txKey types.TxKey) *mempoolTx {
	if e, ok := mem.getCElement(txKey); ok {
		return e.Value.(*mempoolTx)
	}
	return nil
}

func (mem *CListMempool) removeAllTxs() {
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		mem.txs.Remove(e)
		e.DetachPrev()
	}

	mem.txsMap.Range(func(key, _ interface{}) bool {
		mem.txsMap.Delete(key)
		return true
	})
}

// NOTE: not thread safe - should only be called once, on startup
func (mem *CListMempool) EnableTxsAvailable() {
	mem.txsAvailable = make(chan struct{}, 1)
}

// SetLogger sets the Logger.
func (mem *CListMempool) SetLogger(l log.Logger) {
	mem.logger = l
}

// WithPreCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran before CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithPreCheck(f PreCheckFunc) CListMempoolOption {
	return func(mem *CListMempool) { mem.preCheck = f }
}

// WithPostCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran after CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithPostCheck(f PostCheckFunc) CListMempoolOption {
	return func(mem *CListMempool) { mem.postCheck = f }
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *Metrics) CListMempoolOption {
	return func(mem *CListMempool) { mem.metrics = metrics }
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Lock() {
	if mem.recheck.setRecheckFull() {
		mem.logger.Debug("the state of recheckFull has flipped")
	}
	mem.updateMtx.Lock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Unlock() {
	mem.updateMtx.Unlock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Size() int {
	return mem.txs.Len()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) SizeBytes() int64 {
	return mem.txsBytes.Load()
}

// Lock() must be help by the caller during execution.
func (mem *CListMempool) FlushAppConn() error {
	err := mem.proxyAppConn.Flush(context.TODO())
	if err != nil {
		return ErrFlushAppConn{Err: err}
	}

	return nil
}

// XXX: Unsafe! Calling Flush may leave mempool in inconsistent state.
func (mem *CListMempool) Flush() {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	mem.txsBytes.Store(0)
	mem.cache.Reset()

	mem.removeAllTxs()
}

// TxsFront returns the first transaction in the ordered list for peer
// goroutines to call .NextWait() on.
// FIXME: leaking implementation details!
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) TxsFront() *clist.CElement {
	return mem.txs.Front()
}

// TxsWaitChan returns a channel to wait on transactions. It will be closed
// once the mempool is not empty (ie. the internal `mem.txs` has at least one
// element)
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) TxsWaitChan() <-chan struct{} {
	return mem.txs.WaitChan()
}

// It blocks if we're waiting on Update() or Reap().
// cb: A callback from the CheckTx command.
//
//	It gets called from another goroutine.
//
// CONTRACT: Either cb will get called, or err returned.
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) CheckTx(
	tx types.Tx,
	cb func(*abci.ResponseCheckTx),
	txInfo TxInfo,
) error {
	mem.updateMtx.RLock()
	// Unlock will happen in the callback
	txSize := len(tx)

	if err := mem.isFull(txSize); err != nil {
		mem.metrics.RejectedTxs.Add(1)
		return err
	}

	if txSize > mem.config.MaxTxBytes {
		return ErrTxTooLarge{
			Max:    mem.config.MaxTxBytes,
			Actual: txSize,
		}
	}

	if mem.preCheck != nil {
		if err := mem.preCheck(tx); err != nil {
			return ErrPreCheck{Err: err}
		}
	}

	// NOTE: proxyAppConn may error if tx buffer is full
	if err := mem.proxyAppConn.Error(); err != nil {
		return ErrAppConnMempool{Err: err}
	}

	if !mem.cache.Push(tx) { // if the transaction already exists in the cache
		// Record a new sender for a tx we've already seen.
		// Note it's possible a tx is still in the cache but no longer in the mempool
		// (eg. after committing a block, txs are removed from mempool but not cache),
		// so we only record the sender for txs still in the mempool.
		if memTx := mem.getMemTx(tx.Key()); memTx != nil {
			memTx.addSender(txInfo.SenderID)
			// TODO: consider punishing peer for dups,
			// its non-trivial since invalid txs can become valid,
			// but they can spam the same tx with little cost to them atm.
		}
		return ErrTxInCache
	}

	reqRes, err := mem.proxyAppConn.CheckTxAsync(context.TODO(), &abci.RequestCheckTx{Tx: tx})
	if err != nil {
		panic(fmt.Errorf("CheckTx request for tx %s failed: %w", log.NewLazySprintf("%v", tx.Hash()), err))
	}
	reqRes.SetCallback(mem.checkTxCb(tx, txInfo, cb))

	return nil
}

// Request specific callback that should be set on individual reqRes objects
// to incorporate local information when processing the response.
// This allows us to track the peer that sent us this tx, so we can avoid sending it back to them.
// NOTE: alternatively, we could include this information in the ABCI request itself.
//
// External callers of CheckTx, like the RPC, can also pass an externalCb through here that is called
// when all other response processing is complete.
//
// Used in CheckTx to record PeerID who sent us the tx.
func (mem *CListMempool) checkTxCb(
	tx []byte,
	txInfo TxInfo,
	externalCb func(*abci.ResponseCheckTx),
) func(res *abci.Response) {
	return func(res *abci.Response) {
		defer mem.updateMtx.RUnlock()

		if !mem.recheck.done() {
			panic(log.NewLazySprintf("rechecking has not finished; cannot check new tx %v",
				types.Tx(tx).Hash()))
		}

		mem.resCbFirstTime(tx, txInfo, res)

		// update metrics
		mem.metrics.Size.Set(float64(mem.Size()))
		mem.metrics.SizeBytes.Set(float64(mem.SizeBytes()))

		// passed in by the caller of CheckTx, eg. the RPC
		if externalCb != nil {
			externalCb(res.GetCheckTx())
		}
	}
}

// Called from:
//   - resCbFirstTime (lock not held) if tx is valid
func (mem *CListMempool) addTx(memTx *mempoolTx) {
	e := mem.txs.PushBack(memTx)
	mem.txsMap.Store(memTx.tx.Key(), e)
	mem.txsBytes.Add(int64(len(memTx.tx)))
	mem.metrics.TxSizeBytes.Observe(float64(len(memTx.tx)))
}

// RemoveTxByKey removes a transaction from the mempool by its TxKey index.
// Called from:
//   - Update (lock held) if tx was committed
//   - resCbRecheck (lock not held) if tx was invalidated
func (mem *CListMempool) RemoveTxByKey(txKey types.TxKey) error {
	if elem, ok := mem.getCElement(txKey); ok {
		mem.txs.Remove(elem)
		elem.DetachPrev()
		mem.txsMap.Delete(txKey)
		tx := elem.Value.(*mempoolTx).tx
		mem.txsBytes.Add(int64(-len(tx)))
		return nil
	}
	return ErrTxNotFound
}

func (mem *CListMempool) isFull(txSize int) error {
	memSize := mem.Size()
	txsBytes := mem.SizeBytes()
	if memSize >= mem.config.Size || int64(txSize)+txsBytes > mem.config.MaxTxsBytes {
		return ErrMempoolIsFull{
			NumTxs:      memSize,
			MaxTxs:      mem.config.Size,
			TxsBytes:    txsBytes,
			MaxTxsBytes: mem.config.MaxTxsBytes,
		}
	}

	if mem.recheck.consideredFull() {
		return ErrRecheckFull
	}

	return nil
}

// callback, which is called after the app checked the tx for the first time.
//
// The case where the app checks the tx for the second and subsequent times is
// handled by the resCbRecheck callback.
func (mem *CListMempool) resCbFirstTime(
	tx []byte,
	txInfo TxInfo,
	res *abci.Response,
) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(tx, r.CheckTx)
		}
		if (r.CheckTx.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Check mempool isn't full again to reduce the chance of exceeding the
			// limits.
			if err := mem.isFull(len(tx)); err != nil {
				// remove from cache (mempool might have a space later)
				mem.cache.Remove(tx)
				// use debug level to avoid spamming logs when traffic is high
				mem.logger.Debug(err.Error())
				mem.metrics.RejectedTxs.Add(1)
				return
			}

			// Check transaction not already in the mempool
			if e, ok := mem.txsMap.Load(types.Tx(tx).Key()); ok {
				memTx := e.(*clist.CElement).Value.(*mempoolTx)
				memTx.addSender(txInfo.SenderID)
				mem.logger.Debug(
					"transaction already there, not adding it again",
					"tx", types.Tx(tx).Hash(),
					"res", r,
					"height", mem.height.Load(),
					"total", mem.Size(),
				)
				mem.metrics.RejectedTxs.Add(1)
				return
			}

			memTx := &mempoolTx{
				height:    mem.height.Load(),
				gasWanted: r.CheckTx.GasWanted,
				tx:        tx,
			}
			memTx.addSender(txInfo.SenderID)
			mem.addTx(memTx)
			mem.logger.Debug(
				"added good transaction",
				"tx", types.Tx(tx).Hash(),
				"res", r,
				"height", mem.height.Load(),
				"total", mem.Size(),
			)
			mem.notifyTxsAvailable()
		} else {
			// ignore bad transaction
			mem.logger.Debug(
				"rejected bad transaction",
				"tx", types.Tx(tx).Hash(),
				"peerID", txInfo.SenderP2PID,
				"res", r,
				"err", postCheckErr,
			)
			mem.metrics.FailedTxs.Add(1)

			if !mem.config.KeepInvalidTxsInCache {
				// remove from cache (it might be good later)
				mem.cache.Remove(tx)
			}
		}

	default:
		// ignore other messages
	}
}

// callback, which is called after the app rechecked the tx.
//
// The case where the app checks the tx for the first time is handled by the
// resCbFirstTime callback.
func (mem *CListMempool) resCbRecheck(tx types.Tx, res *abci.ResponseCheckTx) {
	// Check whether tx is still in the list of transactions that can be rechecked.
	if !mem.recheck.findNextEntryMatching(&tx) {
		// Reached the end of the list and didn't find a matching tx; rechecking has finished.
		return
	}

	var postCheckErr error
	if mem.postCheck != nil {
		postCheckErr = mem.postCheck(tx, res)
	}

	if (res.Code != abci.CodeTypeOK) || postCheckErr != nil {
		// Tx became invalidated due to newly committed block.
		mem.logger.Debug("tx is no longer valid", "tx", tx.Hash(), "res", res, "postCheckErr", postCheckErr)
		if err := mem.RemoveTxByKey(tx.Key()); err != nil {
			mem.logger.Debug("Transaction could not be removed from mempool", "err", err)
		}
		if !mem.config.KeepInvalidTxsInCache {
			mem.cache.Remove(tx)
			mem.metrics.EvictedTxs.Add(1)
		}
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) TxsAvailable() <-chan struct{} {
	return mem.txsAvailable
}

func (mem *CListMempool) notifyTxsAvailable() {
	if mem.Size() == 0 {
		panic("notified txs available but mempool is empty!")
	}
	if mem.txsAvailable != nil && mem.notifiedTxsAvailable.CompareAndSwap(false, true) {
		// channel cap is 1, so this will send once
		select {
		case mem.txsAvailable <- struct{}{}:
		default:
		}
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Txs {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	var (
		totalGas    int64
		runningSize int64
	)

	// TODO: we will get a performance boost if we have a good estimate of avg
	// size per tx, and set the initial capacity based off of that.
	// txs := make([]types.Tx, 0, cmtmath.MinInt(mem.txs.Len(), max/mem.avgTxSize))
	txs := make([]types.Tx, 0, mem.txs.Len())
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		memTx := e.Value.(*mempoolTx)

		txs = append(txs, memTx.tx)

		dataSize := types.ComputeProtoSizeForTxs([]types.Tx{memTx.tx})

		// Check total size requirement
		if maxBytes > -1 && runningSize+dataSize > maxBytes {
			return txs[:len(txs)-1]
		}

		runningSize += dataSize

		// Check total gas requirement.
		// If maxGas is negative, skip this check.
		// Since newTotalGas < masGas, which
		// must be non-negative, it follows that this won't overflow.
		newTotalGas := totalGas + memTx.gasWanted
		if maxGas > -1 && newTotalGas > maxGas {
			return txs[:len(txs)-1]
		}
		totalGas = newTotalGas
	}
	return txs
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) ReapMaxTxs(max int) types.Txs {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	if max < 0 {
		max = mem.txs.Len()
	}

	txs := make([]types.Tx, 0, cmtmath.MinInt(mem.txs.Len(), max))
	for e := mem.txs.Front(); e != nil && len(txs) <= max; e = e.Next() {
		memTx := e.Value.(*mempoolTx)
		txs = append(txs, memTx.tx)
	}
	return txs
}

// Lock() must be help by the caller during execution.
func (mem *CListMempool) Update(
	height int64,
	txs types.Txs,
	txResults []*abci.ExecTxResult,
	preCheck PreCheckFunc,
	postCheck PostCheckFunc,
) error {
	mem.logger.Debug("Update", "height", height, "len(txs)", len(txs))

	// Set height
	mem.height.Store(height)
	mem.notifiedTxsAvailable.Store(false)

	if preCheck != nil {
		mem.preCheck = preCheck
	}
	if postCheck != nil {
		mem.postCheck = postCheck
	}

	for i, tx := range txs {
		if txResults[i].Code == abci.CodeTypeOK {
			// Add valid committed tx to the cache (if missing).
			_ = mem.cache.Push(tx)
		} else if !mem.config.KeepInvalidTxsInCache {
			// Allow invalid transactions to be resubmitted.
			mem.cache.Remove(tx)
		}

		// Remove committed tx from the mempool.
		//
		// Note an evil proposer can drop valid txs!
		// Mempool before:
		//   100 -> 101 -> 102
		// Block, proposed by an evil proposer:
		//   101 -> 102
		// Mempool after:
		//   100
		// https://github.com/tendermint/tendermint/issues/3322.
		if err := mem.RemoveTxByKey(tx.Key()); err != nil {
			mem.logger.Debug("Committed transaction not in local mempool (not an error)",
				"key", tx.Key(),
				"error", err.Error())
		}
	}

	// Recheck txs left in the mempool to remove them if they became invalid in the new state.
	//if mem.config.Recheck {
	mem.recheckTxs()
	//}

	// Notify if there are still txs left in the mempool.
	if mem.Size() > 0 {
		mem.notifyTxsAvailable()
	}

	// Update metrics
	mem.metrics.Size.Set(float64(mem.Size()))
	mem.metrics.SizeBytes.Set(float64(mem.SizeBytes()))

	return nil
}

// recheckTxs sends all transactions in the mempool to the app for re-validation. When the function
// returns, all recheck responses from the app have been processed.
func (mem *CListMempool) recheckTxs() {
	mem.logger.Info("recheck txs", "height", mem.height.Load(), "num-txs", mem.Size())

	if mem.Size() <= 0 {
		return
	}

	startTs := time.Now()

	mem.recheck.init(mem.txs.Front(), mem.txs.Back())

	// NOTE: globalCb may be called concurrently, but CheckTx cannot be executed concurrently
	// because this function has the lock (via Update and Lock).
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		tx := e.Value.(*mempoolTx).tx
		mem.recheck.numPendingTxs.Add(1)
		waitResponse := &waitRecheckTxResponse{
			tx:     tx,
			waitCb: make(chan *abci.ResponseCheckTx),
		}
		mem.recheck.responseWaitQueue = append(mem.recheck.responseWaitQueue, waitResponse)

		mem.recheckTxAsync(waitResponse)
	}

	// Flush any pending asynchronous recheck requests to process.
	mem.proxyAppConn.Flush(context.TODO())

	go mem.waitForRecheckCallbacks()

	// Give some time to finish processing the responses; then finish the rechecking process, even
	// if not all txs were rechecked.
	select {
	case <-time.After(mem.config.RecheckTimeout):
		mem.recheck.setDone()
		mem.logger.Error("timed out waiting for recheck responses")
	case <-mem.recheck.doneCh:
	}

	if n := mem.recheck.numPendingTxs.Load(); n > 0 {
		mem.logger.Error("not all txs were rechecked", "not-rechecked", n)
	}
	mem.logger.Info("done rechecking txs", "height", mem.height.Load(), "num-txs", mem.Size(), "duration", time.Since(startTs).String())
}

func (mem *CListMempool) recheckTxAsync(waitResponse *waitRecheckTxResponse) {
	// Send a CheckTx request to the app. If we're using a sync client, the resCbRecheck
	// callback will be called right after receiving the response.
	reqRes, err := mem.proxyAppConn.CheckTxAsync(context.TODO(), &abci.RequestCheckTx{
		Tx:   waitResponse.tx,
		Type: abci.CheckTxType_Recheck,
	})
	if err != nil {
		panic(fmt.Errorf("async (re-)CheckTx request for tx %s failed: %w", log.NewLazySprintf("%v", waitResponse.tx.Hash()), err))
	}
	reqRes.SetCallback(mem.recheckTxCb(waitResponse))
}

func (mem *CListMempool) recheckTxSync(waitResponse *waitRecheckTxResponse) {
	// Send a CheckTx request to the app
	res, _ := mem.proxyAppConn.CheckTx(context.TODO(), &abci.RequestCheckTx{
		Tx:   waitResponse.tx,
		Type: abci.CheckTxType_Recheck,
	})
	mem.recheck.numPendingTxs.Add(-1)
	mem.resCbRecheck(waitResponse.tx, res)
}

// recheckTxCb encapsulates in a closure wait channel to return the result of recheckTx to it
func (mem *CListMempool) recheckTxCb(waitResponse *waitRecheckTxResponse) func(*abci.Response) {
	return func(res *abci.Response) {
		switch r := res.Value.(type) {
		case *abci.Response_CheckTx:
			if mem.recheck.done() {
				mem.logger.Error("rechecking has finished; discard late recheck response",
					"tx", log.NewLazySprintf("%v", waitResponse.tx.Key()))
				return
			}
			mem.metrics.RecheckTimes.Add(1)
			waitResponse.waitCb <- r.CheckTx
			// update metrics
			mem.metrics.Size.Set(float64(mem.Size()))

		case *abci.Response_Exception: // optimistic recheck failed, retry this txn sequentially
			if mem.recheck.done() {
				mem.logger.Error("rechecking has finished; discard late optimistic recheck ERROR response",
					"tx", log.NewLazySprintf("%v", waitResponse.tx.Key()))
				return
			}
			mem.metrics.RecheckTimes.Add(1)
			// re-check this txn sequentialy
			waitResponse.waitCb <- nil

		default:
			// ignore other messages
		}
	}
}

func (mem *CListMempool) queueForRecheckTxSync(waitResponse *waitRecheckTxResponse) {
	mem.recheck.reRecheckQueue.PushBack(waitResponse)
}

// reRecheck is only triggered if we failed optimistic recheck for some txns.
// Resetting recheck cursor to point to leftover txns in the mempool (that includes
// both failed optimistic recheck and passed GOOD txns left in the mempool, but it's OK since
// we will just skip them during iteration), and check them synchronously
func (mem *CListMempool) reRecheck() {
	mem.recheck.cursor = mem.txs.Front()
	mem.recheck.end = mem.txs.Back()
	for iter := mem.recheck.reRecheckQueue.Front(); iter != nil; iter = iter.Next() {
		waitResponse := iter.Value.(*waitRecheckTxResponse)
		mem.recheckTxSync(waitResponse)
	}
	mem.recheck.setDone()
}

// waitForRecheckCallbacks iterates over responseWaitQueue channels to handle recheckTx responses in order
func (mem *CListMempool) waitForRecheckCallbacks() {
	for _, waitResponse := range mem.recheck.responseWaitQueue {
		res := <-waitResponse.waitCb
		if res == nil {
			mem.queueForRecheckTxSync(waitResponse)
		} else {
			mem.recheck.numPendingTxs.Add(-1)
			mem.resCbRecheck(waitResponse.tx, res)
		}
	}

	if mem.recheck.done() {
		return
	}

	if mem.recheck.numPendingTxs.Load() > 0 {
		mem.reRecheck()
	} else {
		mem.recheck.setDone()
	}
}

// The cursor and end pointers define a dynamic list of transactions that could be rechecked. The
// end pointer is fixed. When a recheck response for a transaction is received, cursor will point to
// the entry in the mempool corresponding to that transaction, thus narrowing the list. Transactions
// corresponding to entries between the old and current positions of cursor will be ignored for
// rechecking. This is to guarantee that recheck responses are processed in the same sequential
// order as they appear in the mempool.
type recheck struct {
	cursor            *clist.CElement          // next expected recheck response
	end               *clist.CElement          // last entry in the mempool to recheck
	doneCh            chan struct{}            // to signal that rechecking has finished successfully (for async app connections)
	numPendingTxs     atomic.Int32             // number of transactions still pending to recheck
	responseWaitQueue []*waitRecheckTxResponse // array of channels that we use to wait for callbacks in the same order we started the recheck requests
	reRecheckQueue    *clist.CList             // a list of txns awaiting re-recheck (failed during optimistic recheck)
	isRechecking      atomic.Bool              // true iff the rechecking process has begun and is not yet finished
	recheckFull       atomic.Bool              // whether rechecking TXs cannot be completed before a new block is decided
}

type waitRecheckTxResponse struct {
	tx     types.Tx
	waitCb chan *abci.ResponseCheckTx
}

func newRecheck() *recheck {
	return &recheck{
		doneCh:         make(chan struct{}, 1),
		reRecheckQueue: clist.New(),
	}
}

func (rc *recheck) init(first, last *clist.CElement) {
	if !rc.done() {
		panic("Having more than one rechecking process at a time is not possible.")
	}
	rc.cursor = first
	rc.end = last
	rc.numPendingTxs.Store(0)
	rc.responseWaitQueue = make([]*waitRecheckTxResponse, 0)
	rc.reRecheckQueue = clist.New()
	rc.isRechecking.Store(true)
}

// done returns true when there is no recheck response to process.
// Safe for concurrent use by multiple goroutines.
func (rc *recheck) done() bool {
	return !rc.isRechecking.Load()
}

// setDone registers that rechecking has finished.
func (rc *recheck) setDone() {
	rc.recheckFull.Store(false)
	rc.isRechecking.Store(false)

	select {
	case rc.doneCh <- struct{}{}:
	default:
	}
}

// findNextEntryMatching searches for the next transaction matching the given transaction, which
// corresponds to the recheck response to be processed next.
//
// The goal is to guarantee that transactions are rechecked in the order in which they are in the
// mempool.
func (rc *recheck) findNextEntryMatching(tx *types.Tx) bool {
	for ; !rc.done() && rc.cursor != nil; rc.cursor = rc.cursor.Next() {
		expectedTx := rc.cursor.Value.(*mempoolTx).tx
		if bytes.Equal(*tx, expectedTx) {
			return true
		}
	}

	return false
}

// setRecheckFull sets recheckFull to true if rechecking is still in progress. It returns true iff
// the value of recheckFull has changed.
func (rc *recheck) setRecheckFull() bool {
	rechecking := !rc.done()
	recheckFull := rc.recheckFull.Swap(rechecking)
	return rechecking != recheckFull
}

// consideredFull returns true iff the mempool should be considered as full while rechecking is in
// progress.
func (rc *recheck) consideredFull() bool {
	return rc.recheckFull.Load()
}
