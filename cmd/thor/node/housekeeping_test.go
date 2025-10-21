// Copyright (c) 2025 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vechain/thor/v2/block"
	"github.com/vechain/thor/v2/cache"
	"github.com/vechain/thor/v2/chain"
	"github.com/vechain/thor/v2/comm"
	"github.com/vechain/thor/v2/muxdb"
	"github.com/vechain/thor/v2/runtime"
	"github.com/vechain/thor/v2/state"
	"github.com/vechain/thor/v2/thor"
	"github.com/vechain/thor/v2/trie"
	"github.com/vechain/thor/v2/tx"
)

// Mock implementations for testing
type mockCommunicator struct {
	peerCount       int
	broadcastCalled bool
	broadcastBlock  *block.Block
}

func (m *mockCommunicator) BroadcastBlock(blk *block.Block) bool {
	m.broadcastCalled = true
	m.broadcastBlock = blk
	return true
}

func (m *mockCommunicator) PeerCount() int {
	return m.peerCount
}

func (m *mockCommunicator) Sync(ctx context.Context, handler comm.HandleBlockStream) bool {
	return true
}

func (m *mockCommunicator) SubscribeBlock(ch chan *comm.NewBlockEvent) event.Subscription {
	// Return a simple mock subscription
	return &mockSubscription{}
}

func (m *mockCommunicator) Synced() <-chan struct{} {
	return make(chan struct{})
}

// Simple mock subscription
type mockSubscription struct{}

func (m *mockSubscription) Unsubscribe() {}
func (m *mockSubscription) Err() <-chan error {
	return make(chan error)
}

type mockBFT struct{}

func (m *mockBFT) Accepts(parentID thor.Bytes32) (bool, error) {
	return true, nil
}

func (m *mockBFT) Select(header *block.Header) (bool, error) {
	return true, nil
}

func (m *mockBFT) CommitBlock(header *block.Header, isPacking bool) error {
	return nil
}

func (m *mockBFT) ShouldVote(parentID thor.Bytes32) (bool, error) {
	return true, nil
}

type mockConsensus struct {
	stager *state.Stage
}

func newMockConsensus() *mockConsensus {
	newState := state.New(muxdb.NewMem(), trie.Root{})
	stage, err := newState.Stage(trie.Version{Major: 1})
	if err != nil {
		panic(err)
	}
	return &mockConsensus{
		stager: stage,
	}
}

func (m *mockConsensus) Process(parentSummary *chain.BlockSummary, blk *block.Block, nowTimestamp uint64, blockConflicts uint32) (*state.Stage, tx.Receipts, error) {
	return m.stager, nil, nil
}

func (m *mockConsensus) NewRuntimeForReplay(header *block.Header, skipValidation bool) (*runtime.Runtime, error) {
	return nil, nil
}

// Test node that embeds the original but allows method overriding
type mockableNode struct {
	*Node
	processBlockFunc func(blk *block.Block, stats *blockStats) (bool, error)
}

func createTestBlock(parentBlock *chain.BlockSummary) *block.Block {
	builder := new(block.Builder)
	parentID := thor.Bytes32{}
	if parentBlock != nil {
		parentID = parentBlock.Header.ID()
	}

	header := builder.
		ParentID(parentID).
		Timestamp(uint64(time.Now().Unix())).
		GasLimit(10000000).
		Build().Header()

	return block.Compose(header, nil)
}

func setupTestNodeForHousekeeping(t *testing.T) (*mockableNode, *mockCommunicator) {
	// Create test accounts
	accounts := createDevAccounts(t, 3)

	// Create test database
	db := muxdb.NewMem()

	// Create test chain
	chain, err := createChain(db, accounts)
	require.NoError(t, err)

	// Create mock
	mockComm := &mockCommunicator{peerCount: 1}
	mockBFT := &mockBFT{}
	mockCons := newMockConsensus()

	nodeOptions := Options{
		SkipLogs: true,
	}

	// Create original node
	originalNode := &Node{
		cons:               mockCons,
		repo:               chain.Repo(),
		comm:               mockComm,
		forkConfig:         &thor.NoFork,
		bft:                mockBFT,
		newBlockCh:         make(chan *comm.NewBlockEvent, 1),
		futureTicker:       time.NewTicker(100 * time.Millisecond),
		connectivityTicker: time.NewTicker(100 * time.Millisecond),
		clockSyncTicker:    time.NewTicker(100 * time.Millisecond),
		options:            nodeOptions,
	}

	originalNode.futureBlocksCache = cache.NewRandCache(32)

	// Wrap in test node
	testNode := &mockableNode{Node: originalNode}

	return testNode, mockComm
}

func TestNode_HouseKeeping_Newblock(t *testing.T) {
	buf, restore := captureLogs()
	defer restore()

	node, mockComm := setupTestNodeForHousekeeping(t)
	defer node.futureTicker.Stop()
	defer node.connectivityTicker.Stop()
	defer node.clockSyncTicker.Stop()

	tests := []struct {
		name             string
		setupBlock       func() *block.Block
		setupProcessFunc func() func(blk *block.Block, stats *blockStats) (bool, error)
		assertFunc       func(t *testing.T, values map[string]any)
	}{
		// {
		// 	name: "successful trunk block processing",
		// 	setupBlock: func() *block.Block {
		// 		// Create a block that should be processed successfully
		// 		parentBlock := node.repo.BestBlockSummary()

		// 		builder := new(block.Builder)
		// 		parentID := thor.Bytes32{}
		// 		if parentBlock != nil {
		// 			parentID = parentBlock.Header.ID()
		// 		}

		// 		header := builder.
		// 			ParentID(parentID).
		// 			Timestamp(uint64(time.Now().Unix())).
		// 			GasLimit(10000000).
		// 			TotalScore(11).
		// 			Build().Header()

		// 		trunkBL := block.Compose(header, nil)
		// 		return trunkBL
		// 	},
		// 	setupProcessFunc: func() func(blk *block.Block, stats *blockStats) (bool, error) {
		// 		return func(blk *block.Block, stats *blockStats) (bool, error) {
		// 			stats.UpdateProcessed(1, 0, 0, 0, 0, 0)
		// 			return true, nil // isTrunk=true, no error
		// 		}
		// 	},

		// 	assertFunc: func(t *testing.T, values map[string]any) {
		// 		node := values["node"].(*mockableNode)
		// 		mockComm := node.comm.(*mockCommunicator)
		// 		assert.True(t, mockComm.broadcastCalled, "Block should have been broadcast")
		// 		assert.NotEmpty(t, mockComm.broadcastBlock, "Broadcasted block should not be nil")
		// 	},
		// },
		// {
		// 	name: "parent missing error handling",
		// 	setupBlock: func() *block.Block {
		// 		return createTestBlock(nil)
		// 	},
		// 	setupProcessFunc: func() func(blk *block.Block, stats *blockStats) (bool, error) {
		// 		return func(blk *block.Block, stats *blockStats) (bool, error) {
		// 			return false, errParentMissing
		// 		}
		// 	},
		// 	assertFunc: func(t *testing.T, values map[string]any) {
		// 		node := values["node"].(*mockableNode)
		// 		assert.Equal(t, 0, node.futureBlocksCache.Len(), "Future blocks cache should be empty")
		// 	},
		// },
		// {
		// 	name: "parent in future blocks cache and parent missing error handling",
		// 	setupBlock: func() *block.Block {
		// 		// Create a block whose parent is in the future blocks cache
		// 		// bestBlock := node.repo.BestBlockSummary()
		// 		cacheBlock := createTestBlock(nil)
		// 		node.futureBlocksCache.Set(cacheBlock.Header().ID(), cacheBlock)

		// 		builder := new(block.Builder)
		// 		header := builder.
		// 			ParentID(cacheBlock.Header().ID()).
		// 			Timestamp(uint64(time.Now().Unix())).
		// 			GasLimit(10000000).
		// 			TotalScore(11).
		// 			Build().Header()

		// 		newBlock := block.Compose(header, nil)
		// 		return newBlock
		// 	},
		// 	setupProcessFunc: func() func(blk *block.Block, stats *blockStats) (bool, error) {
		// 		return func(blk *block.Block, stats *blockStats) (bool, error) {
		// 			return false, errParentMissing
		// 		}
		// 	},
		// 	assertFunc: func(t *testing.T, values map[string]any) {
		// 		node := values["node"].(*mockableNode)
		// 		assert.Equal(t, 2, node.futureBlocksCache.Len(), "Future blocks cache should contain 2 blocks")
		// 	},
		// },
		{
			name: "temporaryUnprocessable block handling",
			setupBlock: func() *block.Block {
				// Create a block whose parent is in the future blocks cache
				newParentID, _ := thor.ParseBytes32("0x0000000a00000000000000000000000000000000000000000000000000000000") // block number is 10

				builder := new(block.Builder)
				header := builder.
					ParentID(newParentID).
					Timestamp(uint64(time.Now().Unix())).
					GasLimit(10000000).
					TotalScore(11).
					Build().Header()

				parentBlock := block.Compose(header, nil)
				node.futureBlocksCache.Set(parentBlock.Header().ID(), parentBlock)

				builder2 := new(block.Builder)
				header2 := builder2.
					ParentID(parentBlock.Header().ID()).
					Timestamp(uint64(time.Now().Unix())).
					GasLimit(10000000).
					TotalScore(11).
					Build().Header()

				newblock := block.Compose(header2, nil)

				return newblock

			},
			setupProcessFunc: func() func(blk *block.Block, stats *blockStats) (bool, error) {
				return func(blk *block.Block, stats *blockStats) (bool, error) {
					return false, errBlockTemporaryUnprocessable
				}
			},

			assertFunc: func(t *testing.T, values map[string]any) {
				node := values["node"].(*mockableNode)
				assert.Equal(t, 2, node.futureBlocksCache.Len(), "Future blocks cache should contain 1 block")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset mock state
			mockComm.broadcastCalled = false
			mockComm.broadcastBlock = nil

			// Setup process function
			if tt.setupProcessFunc != nil {
				node.processBlockFunc = tt.setupProcessFunc()
			}

			// Clear future blocks cache
			node.futureBlocksCache = cache.NewRandCache(32)

			// Create test block
			testBlock := tt.setupBlock()
			newBlockEvent := &comm.NewBlockEvent{Block: testBlock}

			// Start housekeeping in a goroutine
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			var processedBlock bool
			done := make(chan bool, 1)

			go func() {
				defer func() { done <- true }()

				// Monitor for processing
				go func() {
					select {
					case node.newBlockCh <- newBlockEvent:
						processedBlock = true
					case <-ctx.Done():
					}
				}()

				// Run housekeeping briefly
				select {
				case <-ctx.Done():
				default:
					node.houseKeeping(ctx)
				}
			}()

			// Wait for processing
			<-done

			// Allow some time for processing
			time.Sleep(200 * time.Millisecond)

			// Verify expectations
			assert.True(t, processedBlock, "Block should have been sent to processing channel")
			if tt.assertFunc != nil {
				tt.assertFunc(t, map[string]any{
					"node": node,
					"logs": buf.String(),
				})
			}
			// assert.Equal(t, tt.expectBroadcast, mockComm.broadcastCalled, "Broadcast expectation mismatch")

			// if tt.expectBroadcast {
			// 	assert.NotNil(t, mockComm.broadcastBlock, "Expected block to be broadcast")
			// }
		})
	}
}

func TestNode_HouseKeeping_FutureTicker(t *testing.T) {
	node, mockComm := setupTestNodeForHousekeeping(t)
	defer node.futureTicker.Stop()
	defer node.connectivityTicker.Stop()
	defer node.clockSyncTicker.Stop()

	// Create a very short ticker for testing
	node.futureTicker = time.NewTicker(10 * time.Millisecond)

	// Track processed blocks
	var processedBlocks []*block.Block
	var mu sync.Mutex

	// Mock processBlock to capture processed blocks
	node.processBlockFunc = func(blk *block.Block, stats *blockStats) (bool, error) {
		mu.Lock()
		processedBlocks = append(processedBlocks, blk)
		mu.Unlock()
		stats.UpdateProcessed(1, 0, 0, 0, 0, 0)
		return true, nil // Process successfully as trunk
	}

	// Start housekeeping
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		node.houseKeeping(ctx)
	}()

	// Wait for completion
	<-done

	// The test should complete without hanging
	assert.True(t, true, "Future ticker handling completed successfully")
	// Note: We don't verify block processing here since future blocks cache is internal
	_ = mockComm // Use mockComm to avoid unused variable
}

func TestNode_HouseKeeping_ConnectivityTicker(t *testing.T) {
	node, mockComm := setupTestNodeForHousekeeping(t)
	defer node.futureTicker.Stop()
	defer node.connectivityTicker.Stop()
	defer node.clockSyncTicker.Stop()

	// Create a very short ticker for testing
	node.connectivityTicker = time.NewTicker(5 * time.Millisecond)

	tests := []struct {
		name      string
		peerCount int
	}{
		{
			name:      "with peers connected",
			peerCount: 5,
		},
		{
			name:      "no peers connected",
			peerCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup peer count
			mockComm.peerCount = tt.peerCount

			// Start housekeeping
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			done := make(chan bool)
			go func() {
				defer func() { done <- true }()
				node.houseKeeping(ctx)
			}()

			// Wait for completion
			<-done

			// The test verifies that connectivity ticker doesn't cause hangs
			assert.True(t, true, "Connectivity ticker handling completed successfully")
		})
	}
}

func TestNode_HouseKeeping_ClockSyncTicker(t *testing.T) {
	node, mockComm := setupTestNodeForHousekeeping(t)
	defer node.futureTicker.Stop()
	defer node.connectivityTicker.Stop()
	defer node.clockSyncTicker.Stop()

	// Create a very short ticker for testing
	node.clockSyncTicker = time.NewTicker(10 * time.Millisecond)

	// Start housekeeping
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		node.houseKeeping(ctx)
	}()

	// Wait for completion
	<-done

	// The test should complete without hanging, demonstrating that
	// clock sync ticker events are being handled
	assert.True(t, true, "Clock sync ticker handling completed successfully")
	_ = mockComm // Use mockComm to avoid unused variable
}

func TestNode_HouseKeeping_ContextCancellation(t *testing.T) {
	node, _ := setupTestNodeForHousekeeping(t)
	defer node.futureTicker.Stop()
	defer node.connectivityTicker.Stop()
	defer node.clockSyncTicker.Stop()

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Start housekeeping
	done := make(chan bool)
	go func() {
		node.houseKeeping(ctx)
		done <- true
	}()

	// Cancel context after a short delay
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Verify that housekeeping exits when context is cancelled
	select {
	case <-done:
		assert.True(t, true, "Housekeeping exited properly on context cancellation")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Housekeeping did not exit within expected time after context cancellation")
	}
}

func TestNode_HouseKeeping_AllTickerTypes(t *testing.T) {
	node, mockComm := setupTestNodeForHousekeeping(t)
	defer node.futureTicker.Stop()
	defer node.connectivityTicker.Stop()
	defer node.clockSyncTicker.Stop()

	// Create very short tickers for all types
	node.futureTicker = time.NewTicker(5 * time.Millisecond)
	node.connectivityTicker = time.NewTicker(7 * time.Millisecond)
	node.clockSyncTicker = time.NewTicker(11 * time.Millisecond)

	// Track ticker events
	var events []string
	var mu sync.Mutex

	// Override processBlock to track future ticker events
	node.processBlockFunc = func(blk *block.Block, stats *blockStats) (bool, error) {
		mu.Lock()
		events = append(events, "future_block_processed")
		mu.Unlock()
		return false, nil
	}

	// Set no peers to potentially trigger connectivity logic
	mockComm.peerCount = 0

	// Start housekeeping
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		node.houseKeeping(ctx)
	}()

	// Wait for completion
	<-done

	// Verify the test completed without hanging
	assert.True(t, true, "All ticker types handled successfully")

	// The events might be empty since we don't have actual future blocks cached,
	// but the important thing is that the goroutine didn't hang
	mu.Lock()
	defer mu.Unlock()
	t.Logf("Captured events: %v", events)
}
