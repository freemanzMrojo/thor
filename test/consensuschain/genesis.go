package consensuschain

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/vechain/thor/v2/genesis"
	"github.com/vechain/thor/v2/thor"
)

// NewGenesis creates a hayabusa consensus chain genesis block for testing.
func NewGenesis(amount int) *genesis.Genesis {
	hayabusaTP := uint32(0)
	largeNo := (*genesis.HexOrDecimal256)(new(big.Int).SetBytes(hexutil.MustDecode("0xffffffffffffffffffffffffffffffffff")))
	fc := thor.ForkConfig{
		VIP191:    0,
		ETH_CONST: 0,
		BLOCKLIST: 0,
		ETH_IST:   0,
		VIP214:    0,
		FINALITY:  0,
		HAYABUSA:  0,
		GALACTICA: 0,
	}
	gen := &genesis.CustomGenesis{
		LaunchTime: uint64(1526400000),
		GasLimit:   thor.InitialGasLimit,
		ExtraData:  "Consensus Chain",
		Params:     genesis.Params{},
		Executor: genesis.Executor{
			Approvers: []genesis.Approver{
				{
					Address:  genesis.DevAccounts()[0].Address,
					Identity: thor.BytesToBytes32([]byte("Solo Block Signer")),
				},
			},
		},
		ForkConfig: &fc,
		Config: &thor.Config{
			BlockInterval:       10,
			LowStakingPeriod:    12,
			MediumStakingPeriod: 30,
			HighStakingPeriod:   90,
			CooldownPeriod:      12,
			EpochLength:         6,
			HayabusaTP:          &hayabusaTP,
		},
	}

	for i := range amount {
		gen.Authority = append(gen.Authority, genesis.Authority{
			MasterAddress:   genesis.DevAccounts()[i].Address,
			EndorsorAddress: genesis.DevAccounts()[i].Address,
			Identity:        thor.BytesToBytes32([]byte(fmt.Sprintf("Solo Block Signer %d", i))),
		})
		gen.Accounts = append(gen.Accounts, genesis.Account{
			Address: genesis.DevAccounts()[i].Address,
			Balance: largeNo,
			Energy:  largeNo,
		})
	}

	customNet, err := genesis.NewCustomNet(gen)
	if err != nil {
		panic(err)
	}
	return customNet
}
