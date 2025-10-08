package consensuschain

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vechain/thor/v2/thor"
)

func TestConsensusChain_PrepareBlock(t *testing.T) {
	config := &Config{
		Nodes:       3,
		ForkConfig:  &thor.ForkConfig{},
		EpochLength: 18,
	}

	chain, err := New(config)
	require.NoError(t, err)

	for range 1000 {
		_, err = chain.PrepareBlock().Mint()
		require.NoError(t, err)
	}
}
