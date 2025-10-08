package consensuschain

import (
	"errors"

	"github.com/vechain/thor/v2/block"
	"github.com/vechain/thor/v2/packer"
	"github.com/vechain/thor/v2/test/testchain"
	"github.com/vechain/thor/v2/thor"
	"github.com/vechain/thor/v2/tx"
)

type BlockBuilder struct {
	chain       *ConsensusChain
	txs         []*tx.Transaction
	missedSlots uint64
}

func NewBlockBuilder(chain *ConsensusChain) *BlockBuilder {
	return &BlockBuilder{
		chain: chain,
		txs:   make([]*tx.Transaction, 0),
	}
}

func (b *BlockBuilder) Add(tx *tx.Transaction) *BlockBuilder {
	b.txs = append(b.txs, tx)
	return b
}

func (b *BlockBuilder) MissedSlots(amount uint64) *BlockBuilder {
	b.missedSlots = amount
	return b
}

func (b *BlockBuilder) anyChain() *testchain.Chain {
	for _, chain := range b.chain.nodes {
		return chain
	}
	return nil
}

func (b *BlockBuilder) Mint() (*block.Block, error) {
	parent := b.anyChain().Repo().BestBlockSummary()
	timestamp := parent.Header.Timestamp() + (1+b.missedSlots)*thor.BlockInterval()

	var (
		flow   *packer.Flow
		when   uint64
		signer thor.Address
	)

	for addr, packer := range b.chain.packers {
		currentFlow, _, err := packer.Schedule(parent, timestamp-1) // -1 so that time now is before next timestamp
		if err != nil {
			return nil, err
		}
		if when == 0 || currentFlow.When() < when {
			when = currentFlow.When()
			flow = currentFlow
			signer = addr
		}
	}

	if flow == nil {
		return nil, errors.New("unexpected error finding the correct flow")
	}

	for _, tx := range b.txs {
		if err := flow.Adopt(tx); err != nil {
			return nil, err
		}
	}

	privateKey, ok := b.chain.keys[signer]
	if !ok {
		return nil, errors.New("failed to find packer private key")
	}
	block, _, _, err := flow.Pack(privateKey, 0, false)
	if err != nil {
		return nil, err
	}

	for _, chain := range b.chain.nodes {
		if err := chain.ProcessBlock(block); err != nil {
			return nil, err
		}
	}

	return nil, nil
}
