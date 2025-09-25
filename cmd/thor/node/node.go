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
	"github.com/vechain/thor/v2/builtin"
	"github.com/vechain/thor/v2/cache"
	"github.com/vechain/thor/v2/chain"
	"github.com/vechain/thor/v2/cmd/thor/bandwidth"
	"github.com/vechain/thor/v2/co"
	"github.com/vechain/thor/v2/comm"
	"github.com/vechain/thor/v2/consensus"
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

// guardBlockProcessing adds lock on block processing and maintains block conflicts.
func (n *Node) guardBlockProcessing(blockNum uint32, process func(conflicts uint32) (thor.Bytes32, error)) error {
	n.processLock.Lock()
	defer n.processLock.Unlock()

	var (
		err       error
		blockID   thor.Bytes32
		conflicts []thor.Bytes32
	)

	if blockNum > n.maxBlockNum {
		if blockNum > n.maxBlockNum+1 {
			// the block is surely unprocessable now
			return errBlockTemporaryUnprocessable
		}

		// don't increase maxBlockNum if the block is unprocessable
		if blockID, err = process(0); err != nil {
			return err
		}

		if n.initialSynced {
			if needPrintWelcomeInfo() &&
				blockNum >= n.forkConfig.HAYABUSA+thor.HayabusaTP() &&
				// if transition period are set to 0, transition will attempt to happen on every block
				(thor.HayabusaTP() == 0 || (blockNum-n.forkConfig.HAYABUSA)%thor.HayabusaTP() == 0) {
				summary, err := n.repo.GetBlockSummary(blockID)
				if err != nil {
					return err
				}
				state := n.stater.NewState(summary.Root())
				active, err := builtin.Staker.Native(state).IsPoSActive()
				if err != nil {
					return nil
				}
				if active {
					printWelcomeInfo()
				}
			}
		}

		n.maxBlockNum = blockNum
		return nil
	}

	conflicts, err = n.repo.GetConflicts(blockNum)
	if err != nil {
		return err
	}
	blockID, err = process(uint32(len(conflicts)))
	if err != nil {
		return err
	}

	// post process block hook, executed only if the block is processed successfully
	if err = func() error {
		if len(conflicts) > 0 {
			newBlock, err := n.repo.GetBlock(blockID)
			if err != nil {
				return err
			}

			newSigner, err := newBlock.Header().Signer()
			if err != nil {
				return err
			}
			// iter over conflicting blocks
			for _, conflict := range conflicts {
				conflictBlock, err := n.repo.GetBlock(conflict)
				if err != nil {
					return err
				}
				// logic to verify that the blocks are different and from the same signer
				existingSigner, err := conflictBlock.Header().Signer()
				if err != nil {
					return err
				}
				if existingSigner == newSigner &&
					conflictBlock.Header().ID() != newBlock.Header().ID() {
					log.Warn("Double signing", "block", shortID(newBlock.Header().ID()), "previous", shortID(conflict), "signer", existingSigner)
					metricDoubleSignedBlocks().AddWithLabel(1, map[string]string{"signer": existingSigner.String()})
				}
			}
		}

		return nil
	}(); err != nil {
		logger.Warn("failed to run post process hook", "err", err)
	}

	return nil
}

func (n *Node) processBlock(newBlock *block.Block, stats *blockStats) (bool, error) {
	var isTrunk *bool

	if err := n.guardBlockProcessing(newBlock.Header().Number(), func(conflicts uint32) (thor.Bytes32, error) {
		// Check whether the block was already there.
		// It can be skipped if no conflicts.
		if conflicts > 0 {
			_, err := n.repo.GetBlockSummary(newBlock.Header().ID())
			if err != nil {
				if !n.repo.IsNotFound(err) {
					return thor.Bytes32{}, err
				}
			} else {
				return thor.Bytes32{}, errKnownBlock
			}
		}
		parentSummary, err := n.repo.GetBlockSummary(newBlock.Header().ParentID())
		if err != nil {
			if !n.repo.IsNotFound(err) {
				return thor.Bytes32{}, err
			}
			return thor.Bytes32{}, errParentMissing
		}

		var (
			startTime = mclock.Now()
			oldBest   = n.repo.BestBlockSummary()
		)

		if ok, err := n.bft.Accepts(newBlock.Header().ParentID()); err != nil {
			return thor.Bytes32{}, errors.Wrap(err, "bft accepts")
		} else if !ok {
			return thor.Bytes32{}, errBFTRejected
		}

		// process the new block
		stage, receipts, err := n.cons.Process(parentSummary, newBlock, uint64(time.Now().Unix()), conflicts)
		if err != nil {
			return thor.Bytes32{}, err
		}

		var becomeNewBest bool
		// let bft engine decide the best block after fork FINALITY
		if newBlock.Header().Number() >= n.forkConfig.FINALITY && oldBest.Header.Number() >= n.forkConfig.FINALITY {
			becomeNewBest, err = n.bft.Select(newBlock.Header())
			if err != nil {
				return thor.Bytes32{}, errors.Wrap(err, "bft select")
			}
		} else {
			becomeNewBest = newBlock.Header().BetterThan(oldBest.Header)
		}
		logEnabled := becomeNewBest && !n.options.SkipLogs && !n.logDBFailed
		isTrunk = &becomeNewBest

		execElapsed := mclock.Now() - startTime

		// write logs
		if logEnabled {
			if err := n.writeLogs(newBlock, receipts, oldBest.Header.ID()); err != nil {
				return thor.Bytes32{}, errors.Wrap(err, "write logs")
			}
		}

		// commit produced states
		if _, err := stage.Commit(); err != nil {
			return thor.Bytes32{}, errors.Wrap(err, "commit state")
		}

		// sync the log-writing task
		if logEnabled {
			if err := n.logWorker.Sync(); err != nil {
				log.Warn("failed to write logs", "err", err)
				n.logDBFailed = true
			}
		}

		// add the new block into repository
		if err := n.repo.AddBlock(newBlock, receipts, conflicts, becomeNewBest); err != nil {
			return thor.Bytes32{}, errors.Wrap(err, "add block")
		}

		// commit block in bft engine
		if newBlock.Header().Number() >= n.forkConfig.FINALITY {
			if err := n.bft.CommitBlock(newBlock.Header(), false); err != nil {
				return thor.Bytes32{}, errors.Wrap(err, "bft commits")
			}
		}

		realElapsed := mclock.Now() - startTime

		if becomeNewBest {
			n.processFork(newBlock, oldBest.Header.ID())
		}

		commitElapsed := mclock.Now() - startTime - execElapsed

		if v, updated := n.bandwidth.Update(newBlock.Header(), time.Duration(realElapsed)); updated {
			logger.Trace("bandwidth updated", "gps", v)
		}
		stats.UpdateProcessed(1, len(receipts), execElapsed, commitElapsed, realElapsed, newBlock.Header().GasUsed())

		metricBlockProcessedTxs().SetWithLabel(int64(len(receipts)), map[string]string{"type": "received"})
		metricBlockProcessedGas().SetWithLabel(int64(newBlock.Header().GasUsed()), map[string]string{"type": "received"})
		metricBlockProcessedDuration().Observe(time.Duration(realElapsed).Milliseconds())
		return newBlock.Header().ID(), nil
	}); err != nil {
		switch {
		case err == errKnownBlock:
			stats.UpdateIgnored(1)
			return false, nil
		case consensus.IsFutureBlock(err) || err == errParentMissing || err == errBlockTemporaryUnprocessable:
			stats.UpdateQueued(1)
		case err == errBFTRejected:
			// TODO: capture metrics
			logger.Debug(fmt.Sprintf("block rejected by BFT engine\n%v\n", newBlock.Header()))
		case consensus.IsCritical(err):
			msg := fmt.Sprintf(`failed to process block due to consensus failure\n%v\n`, newBlock.Header())
			logger.Error(msg, "err", err)
		default:
			logger.Error("failed to process block", "err", err)
		}
		metricBlockProcessedCount().AddWithLabel(1, map[string]string{"type": "received", "success": "false"})
		return false, err
	}
	metricBlockProcessedCount().AddWithLabel(1, map[string]string{"type": "received", "success": "true"})
	return *isTrunk, nil
}

func (n *Node) writeLogs(newBlock *block.Block, newReceipts tx.Receipts, oldBestBlockID thor.Bytes32) (err error) {
	var w *logdb.Writer
	if int64(newBlock.Header().Timestamp()) < time.Now().Unix()-24*3600 {
		// turn off log sync to quickly catch up
		w = n.logDB.NewWriterSyncOff()
	} else {
		w = n.logDB.NewWriter()
	}
	defer func() {
		if err != nil {
			n.logWorker.Run(w.Rollback)
		}
	}()

	oldTrunk := n.repo.NewChain(oldBestBlockID)
	newTrunk := n.repo.NewChain(newBlock.Header().ParentID())

	oldBranch, err := oldTrunk.Exclude(newTrunk)
	if err != nil {
		return err
	}

	// to clear logs on the old branch.
	if len(oldBranch) > 0 {
		n.logWorker.Run(func() error {
			return w.Truncate(block.Number(oldBranch[0]))
		})
	}

	newBranch, err := newTrunk.Exclude(oldTrunk)
	if err != nil {
		return err
	}
	// write logs on the new branch.
	for _, id := range newBranch {
		block, err := n.repo.GetBlock(id)
		if err != nil {
			return err
		}
		receipts, err := n.repo.GetBlockReceipts(id)
		if err != nil {
			return err
		}
		n.logWorker.Run(func() error {
			return w.Write(block, receipts)
		})
	}

	n.logWorker.Run(func() error {
		if err := w.Write(newBlock, newReceipts); err != nil {
			return err
		}
		return w.Commit()
	})
	return nil
}

func (n *Node) processFork(newBlock *block.Block, oldBestBlockID thor.Bytes32) {
	oldTrunk := n.repo.NewChain(oldBestBlockID)
	newTrunk := n.repo.NewChain(newBlock.Header().ParentID())

	sideIDs, err := oldTrunk.Exclude(newTrunk)
	if err != nil {
		logger.Warn("failed to process fork", "err", err)
		return
	}

	metricChainForkCount().Add(int64(len(sideIDs)))

	if len(sideIDs) == 0 {
		return
	}

	if n := len(sideIDs); n >= 2 {
		logger.Warn(fmt.Sprintf(
			`⑂⑂⑂⑂⑂⑂⑂⑂ FORK HAPPENED ⑂⑂⑂⑂⑂⑂⑂⑂
side-chain:   %v  %v`,
			n, sideIDs[n-1]))
	}

	for _, id := range sideIDs {
		b, err := n.repo.GetBlock(id)
		if err != nil {
			logger.Warn("failed to process fork", "err", err)
			return
		}
		for _, tx := range b.Transactions() {
			if err := n.txPool.Add(tx); err != nil {
				logger.Debug("failed to add tx to tx pool", "err", err, "id", tx.ID())
			}
		}
	}
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
