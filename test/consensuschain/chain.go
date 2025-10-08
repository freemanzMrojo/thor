package consensuschain

import (
	"crypto/ecdsa"
	"errors"

	"github.com/vechain/thor/v2/genesis"
	"github.com/vechain/thor/v2/packer"
	"github.com/vechain/thor/v2/test/testchain"
	"github.com/vechain/thor/v2/thor"
)

type Config struct {
	Nodes       int
	ForkConfig  *thor.ForkConfig
	EpochLength uint32
}

type ConsensusChain struct {
	nodes   map[thor.Address]*testchain.Chain
	packers map[thor.Address]*packer.Packer
	keys    map[thor.Address]*ecdsa.PrivateKey
}

func New(config *Config) (*ConsensusChain, error) {
	if config.Nodes > 10 {
		return nil, errors.New("too many nodes")
	}
	gene := NewGenesis(config.Nodes)
	chains := make(map[thor.Address]*testchain.Chain)
	packers := make(map[thor.Address]*packer.Packer)
	keys := make(map[thor.Address]*ecdsa.PrivateKey)
	for i := 0; i < config.Nodes; i++ {
		chain, err := testchain.NewIntegrationTestChainWithGenesis(gene, config.ForkConfig, config.EpochLength)
		if err != nil {
			return nil, err
		}
		acc := genesis.DevAccounts()[i]
		p := packer.New(chain.Repo(), chain.Stater(), acc.Address, &acc.Address, config.ForkConfig, 1)
		chains[acc.Address] = chain
		packers[acc.Address] = p
		keys[acc.Address] = genesis.DevAccounts()[i].PrivateKey
	}
	return &ConsensusChain{
		nodes:   chains,
		packers: packers,
		keys:    keys,
	}, nil
}

func (c *ConsensusChain) PrepareBlock() *BlockBuilder {
	return NewBlockBuilder(c)
}
