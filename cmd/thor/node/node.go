// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beevik/ntp"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/event"
	"github.com/pkg/errors"
	"github.com/vechain/thor/v2/block"
	"github.com/vechain/thor/v2/cache"
	"github.com/vechain/thor/v2/chain"
	"github.com/vechain/thor/v2/cmd/thor/bandwidth"
	"github.com/vechain/thor/v2/co"
	"github.com/vechain/thor/v2/comm"
	"github.com/vechain/thor/v2/log"
	"github.com/vechain/thor/v2/logdb"
	"github.com/vechain/thor/v2/packer"
	"github.com/vechain/thor/v2/state"
	"github.com/vechain/thor/v2/thor"
	"github.com/vechain/thor/v2/tx"
	"github.com/vechain/thor/v2/txpool"
)

var logger = log.WithContext("pkg", "node")

var (
	// error when the block larger than known max block number + 1
	errBlockTemporaryUnprocessable = errors.New("block temporary unprocessable")
	errKnownBlock                  = errors.New("block already in the chain")
	errParentMissing               = errors.New("parent block is missing")
	errBFTRejected                 = errors.New("block rejected by BFT engine")
)

// Options options for tx pool.
type Options struct {
	TargetGasLimit   uint64
	SkipLogs         bool
	MinTxPriorityFee uint64
}

// ConsensusEngine defines the interface for consensus processing
type ConsensusEngine interface {
	Process(parentSummary *chain.BlockSummary, blk *block.Block, nowTimestamp uint64, blockConflicts uint32) (*state.Stage, tx.Receipts, error)
}

// PackerEngine defines the interface for packing blocks
type PackerEngine interface {
	Schedule(parent *chain.BlockSummary, nowTimestamp uint64) (flow *packer.Flow, posActive bool, err error)
	SetTargetGasLimit(gl uint64)
}

// CommunicatorEngine defines the interface for p2p communication
type CommunicatorEngine interface {
	Sync(ctx context.Context, handler comm.HandleBlockStream) bool
	SubscribeBlock(ch chan *comm.NewBlockEvent) event.Subscription
	BroadcastBlock(blk *block.Block) bool
	PeerCount() int
	Synced() <-chan struct{}
}

type RepositoryEngine interface {
	GetMaxBlockNum() (uint32, error)
	ScanConflicts(blockNum uint32) (uint32, error)
	GetBlockSummary(id thor.Bytes32) (*chain.BlockSummary, error)
	IsNotFound(error) bool
	BestBlockSummary() *chain.BlockSummary
	NewChain(head thor.Bytes32) *chain.Chain
	AddBlock(blk *block.Block, receipts tx.Receipts, conflicts uint32, isTrunk bool) error
	GetBlock(id thor.Bytes32) (*block.Block, error)
	GetBlockReceipts(id thor.Bytes32) (tx.Receipts, error)
	NewTicker() co.Waiter
	GetConflicts(blockNum uint32) ([]thor.Bytes32, error)
	ChainTag() byte
	GenesisBlock() *block.Block
	ScanHeads(from uint32) ([]thor.Bytes32, error)
	GetBlockTransactions(id thor.Bytes32) (tx.Transactions, error)
}

type BFTEngine interface {
	Accepts(parentID thor.Bytes32) (bool, error)
	Select(header *block.Header) (bool, error)
	CommitBlock(header *block.Header, isPacking bool) error
	ShouldVote(parentID thor.Bytes32) (bool, error)
}

type TxPoolEngine interface {
	Fill(txs tx.Transactions)
	Add(newTx *tx.Transaction) error
	SubscribeTxEvent(ch chan *txpool.TxEvent) event.Subscription
	Executables() tx.Transactions
	Remove(txHash thor.Bytes32, txID thor.Bytes32) bool
	Close()
	AddLocal(newTx *tx.Transaction) error
	Get(id thor.Bytes32) *tx.Transaction
	StrictlyAdd(newTx *tx.Transaction) error
	Dump() tx.Transactions
	Len() int
}

type Node struct {
	packer      PackerEngine
	cons        ConsensusEngine
	master      *Master
	repo        RepositoryEngine
	bft         BFTEngine
	stater      *state.Stater
	logDB       *logdb.LogDB
	txPool      TxPoolEngine
	txStashPath string
	comm        CommunicatorEngine
	forkConfig  *thor.ForkConfig
	options     Options

	logDBFailed        bool
	initialSynced      bool // true if the initial synchronization process is done
	bandwidth          bandwidth.Bandwidth
	maxBlockNum        uint32
	processLock        sync.Mutex
	logWorker          *worker
	scope              event.SubscriptionScope
	newBlockCh         chan *comm.NewBlockEvent
	txCh               chan *txpool.TxEvent
	futureTicker       *time.Ticker
	connectivityTicker *time.Ticker
	clockSyncTicker    *time.Ticker
	futureBlocksCache  *cache.RandCache
}

func New(
	master *Master,
	repo RepositoryEngine,
	bft BFTEngine,
	stater *state.Stater,
	logDB *logdb.LogDB,
	txPool TxPoolEngine,
	txStashPath string,
	comm CommunicatorEngine,
	forkConfig *thor.ForkConfig,
	options Options,
	consensusEngine ConsensusEngine,
	packerEngine PackerEngine,
) *Node {
	return &Node{
		packer:      packerEngine,
		cons:        consensusEngine,
		master:      master,
		repo:        repo,
		bft:         bft,
		stater:      stater,
		logDB:       logDB,
		txPool:      txPool,
		txStashPath: txStashPath,
		comm:        comm,
		forkConfig:  forkConfig,
		options:     options,
	}
}

func (n *Node) Run(ctx context.Context) error {
	logWorker := newWorker()
	defer logWorker.Close()

	n.logWorker = logWorker

	n.futureBlocksCache = cache.NewRandCache(32)

	maxBlockNum, err := n.repo.GetMaxBlockNum()
	if err != nil {
		return err
	}
	n.maxBlockNum = maxBlockNum

	// Initialization channels
	n.scope = event.SubscriptionScope{}

	n.newBlockCh = make(chan *comm.NewBlockEvent)
	n.scope.Track(n.comm.SubscribeBlock(n.newBlockCh))

	n.txCh = make(chan *txpool.TxEvent)
	n.scope.Track(n.txPool.SubscribeTxEvent(n.txCh))

	defer n.scope.Close()

	// Initialization tickers
	n.futureTicker = time.NewTicker(time.Duration(thor.BlockInterval()) * time.Second)
	defer n.futureTicker.Stop()

	n.connectivityTicker = time.NewTicker(time.Second)
	defer n.connectivityTicker.Stop()

	n.clockSyncTicker = time.NewTicker(10 * time.Minute)
	defer n.clockSyncTicker.Stop()

	var goes co.Goes
	goes.Go(func() { n.comm.Sync(ctx, n.handleBlockStream) })
	goes.Go(func() { n.houseKeeping(ctx) })
	goes.Go(func() { n.txStashLoop(ctx) })
	goes.Go(func() { n.packerLoop(ctx) })

	goes.Wait()

	return nil
}

func (n *Node) handleBlockStream(ctx context.Context, stream <-chan *block.Block) (err error) {
	logger.Debug("start to process block stream")
	defer logger.Debug("process block stream done", "err", err)
	var stats blockStats
	startTime := mclock.Now()

	report := func(block *block.Block) {
		logger.Info(fmt.Sprintf("imported blocks (%v)", stats.processed), stats.LogContext(block.Header())...)
		stats = blockStats{}
		startTime = mclock.Now()
	}

	var blk *block.Block
	for blk = range stream {
		if blk == nil {
			continue
		}
		if _, err := n.processBlock(blk, &stats); err != nil {
			return err
		}

		if stats.processed > 0 &&
			mclock.Now()-startTime > mclock.AbsTime(time.Second*2) {
			report(blk)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if blk != nil && stats.processed > 0 {
		report(blk)
	}
	return nil
}

func checkClockOffset() {
	resp, err := ntp.Query("pool.ntp.org")
	if err != nil {
		logger.Debug("failed to access NTP", "err", err)
		return
	}
	if resp.ClockOffset > time.Duration(thor.BlockInterval())*time.Second/2 {
		logger.Warn("clock offset detected", "offset", common.PrettyDuration(resp.ClockOffset))
	}
}
