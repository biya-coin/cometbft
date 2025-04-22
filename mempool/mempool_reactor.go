package mempool

import (
	"context"
	"errors"
	"sync"
	"time"

	"fmt"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
	"golang.org/x/sync/semaphore"
)

// Reactor handles mempool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type MempoolReactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool Mempool
	ids     *mempoolIDs

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted

	// Map of peer ID to their broadcast channel
	peerBroadcastChannels sync.Map

	// Channel for receiving transactions from the mempool.
	// This channel is used by broadcastTxRoutine to distribute transactions to peers.
	getMempoolTx chan *mempoolTx
}

// NewMempoolReactor returns a new MempoolReactor with the given config and mempool.
func NewMempoolReactor(config *cfg.MempoolConfig, mempool Mempool) p2p.Reactor {
	memR := &MempoolReactor{
		config:                config,
		mempool:               mempool,
		ids:                   newMempoolIDs(),
		peerBroadcastChannels: sync.Map{},
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// InitPeer implements Reactor by creating a state for the peer.
func (memR *MempoolReactor) InitPeer(peer p2p.Peer) p2p.Peer {
	memR.ids.ReserveForPeer(peer)
	return peer
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *MempoolReactor) SetLogger(l log.Logger) {
	memR.Logger = l
}

// OnStart implements p2p.BaseReactor.
func (memR *MempoolReactor) OnStart() error {
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	} else {
		go memR.broadcastTxRoutine()
	}
	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *MempoolReactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *MempoolReactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err != nil {
							continue
						}

						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
						break
					}
				}
			}

			// Check if peer is still running after semaphore acquisition
			if !peer.IsRunning() {
				return
			}

			peerID := memR.ids.GetForPeer(peer)
			peerChan := make(chan *mempoolTx, memR.config.Size)

			// Store the channel atomically
			if _, loaded := memR.peerBroadcastChannels.LoadOrStore(peerID, peerChan); loaded {
				// If channel already exists, close the new one and return
				close(peerChan)
				return
			}

			// Start the broadcast routine
			memR.broadcastTxPeerRoutine(peer, peerChan)
		}()
	}
}

// RemovePeer implements Reactor.
func (memR *MempoolReactor) RemovePeer(peer p2p.Peer, _ interface{}) {
	peerID := memR.ids.GetForPeer(peer)

	if ch, exists := memR.peerBroadcastChannels.LoadAndDelete(peerID); exists {
		close(ch.(chan *mempoolTx))
	}

	memR.ids.Reclaim(peer)
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *MempoolReactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.Txs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("received empty txs from peer", "src", e.Src)
			return
		}
		txInfo := TxInfo{SenderID: memR.ids.GetForPeer(e.Src)}
		if e.Src != nil {
			txInfo.SenderP2PID = e.Src.ID()
		}

		var err error
		for _, tx := range protoTxs {
			ntx := types.Tx(tx)
			err = memR.mempool.CheckTx(ntx, nil, txInfo)
			if err != nil {
				switch {
				case errors.Is(err, ErrTxInCache):
					memR.Logger.Debug("Tx already exists in cache", "tx", ntx.String())
				case errors.As(err, &ErrMempoolIsFull{}):
					// using debug level to avoid flooding when traffic is high
					memR.Logger.Debug(err.Error())
				default:
					memR.Logger.Info("Could not check tx", "tx", ntx.String(), "err", err)
				}
			}
		}
	default:
		memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

// Send new mempool txs to peer.
func (memR *MempoolReactor) broadcastTxPeerRoutine(peer p2p.Peer, peerChan chan *mempoolTx) {
	peerID := memR.ids.GetForPeer(peer)

	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		// Make sure the peer is up to date.
		peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
		if !ok {
			// Peer does not have a state yet. We set it in the consensus reactor, but
			// when we add peer in Switch, the order we call reactors#AddPeer is
			// different every time due to us using a map. Sometimes other reactors
			// will be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		select {
		case memTx, ok := <-peerChan:
			if !ok {
				return
			}

			if peerState.GetHeight() < memTx.Height()-1 {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}

			if !memTx.isSender(peerID) {
				success := peer.Send(p2p.Envelope{
					ChannelID: MempoolChannel,
					Message:   &protomem.Txs{Txs: [][]byte{memTx.tx}},
				})
				if !success {
					time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
					continue
				}
			}
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}

// BroadcastTx sends a transaction to all connected peers
func (memR *MempoolReactor) broadcastTxRoutine() {
	// Check if mempool tx channel is set
	if memR.getMempoolTx == nil {
		memR.Logger.Error("mempool tx channel is not set, broadcasting is disabled")
		return
	}

	for {
		if !memR.IsRunning() {
			return
		}

		select {
		case tx := <-memR.getMempoolTx:
			memR.peerBroadcastChannels.Range(func(key, value interface{}) bool {
				peerID := key.(uint16)
				ch := value.(chan *mempoolTx)

				select {
				case ch <- tx:
				default:
					memR.Logger.Debug("peer broadcast channel is full", "peerID", peerID)
				}
				return true
			})
		case <-memR.Quit():
			return
		}
	}
}

// SetMempoolTxChannel sets the channel for receiving transactions from the mempool.
// This channel will be used by the broadcastTxRoutine to distribute transactions to peers.
// NOTE: This method MUST be called before OnStart() is called.
func (memR *MempoolReactor) SetMempoolTxChannel(channel chan *mempoolTx) {
	memR.getMempoolTx = channel
}
