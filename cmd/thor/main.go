package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/inconshreveable/log15"
	"github.com/vechain/thor/api"
	"github.com/vechain/thor/chain"
	"github.com/vechain/thor/co"
	"github.com/vechain/thor/comm"
	"github.com/vechain/thor/consensus"
	"github.com/vechain/thor/genesis"
	"github.com/vechain/thor/logdb"
	"github.com/vechain/thor/lvldb"
	"github.com/vechain/thor/p2psrv"
	"github.com/vechain/thor/packer"
	"github.com/vechain/thor/state"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/txpool"
	cli "gopkg.in/urfave/cli.v1"
)

var (
	version   = "1.0"
	gitCommit string
	release   = "dev"

	log = log15.New()
)

var boot = "enode://b788e1d863aaea4fecef4aba4be50e59344d64f2db002160309a415ab508977b8bffb7bac3364728f9cdeab00ebdd30e8d02648371faacd0819edc27c18b2aad@106.15.4.191:55555"

// Options for Client.
type Options struct {
	DataPath    string
	Bind        string
	Proposer    thor.Address
	Beneficiary thor.Address
	PrivateKey  *ecdsa.PrivateKey
}

func main() {
	app := cli.NewApp()
	app.Version = fmt.Sprintf("%s-%s-commit%s", release, version, gitCommit)
	app.Name = "Thor"
	app.Usage = "Core of VeChain"
	app.Copyright = "2018 VeChain Foundation <https://vechain.org/>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "port",
			Value: ":56565",
			Usage: "p2p listen port",
		},
		cli.StringFlag{
			Name:  "restfulport",
			Value: ":8081",
			Usage: "restful port",
		},
		cli.StringFlag{
			Name:  "nodekey",
			Usage: "private key (for node) file path (defaults to ~/.thor-node.key if omitted)",
		},
		cli.StringFlag{
			Name:  "key",
			Usage: "private key (for pack) as hex (for testing)",
		},
		cli.StringFlag{
			Name:  "datadir",
			Value: "/tmp/thor_datadir_test",
			Usage: "chain data path",
		},
		cli.IntFlag{
			Name:  "verbosity",
			Value: int(log15.LvlInfo),
			Usage: "log verbosity (0-9)",
		},
	}
	app.Action = action

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func action(ctx *cli.Context) error {
	initLog(log15.Lvl(ctx.Int("verbosity")))

	lv, err := lvldb.New(ctx.String("datadir"), lvldb.Options{})
	if err != nil {
		return err
	}
	defer lv.Close()

	logdb, err := logdb.New(ctx.String("datadir") + "/log.db")
	if err != nil {
		return err
	}
	defer logdb.Close()

	nodeKey, err := loadNodeKey(ctx)
	if err != nil {
		return err
	}

	proposer, privateKey, err := loadAccount(ctx)
	if err != nil {
		return err
	}

	lsr, err := net.Listen("tcp", ctx.String("restfulport"))
	if err != nil {
		return err
	}
	defer lsr.Close()

	//////////
	stateCreator := state.NewCreator(lv)
	genesisBlock, _, err := genesis.Dev.Build(stateCreator)
	if err != nil {
		return err
	}

	chain, err := chain.New(lv, genesisBlock)
	if err != nil {
		return err
	}
	txpool := txpool.New(chain, stateCreator)
	communicator := comm.New(chain, txpool)
	consensus := consensus.New(chain, stateCreator)
	packer := packer.New(chain, stateCreator, proposer, proposer)
	rest := &http.Server{Handler: api.New(chain, stateCreator, txpool, logdb)}
	p2pSrv := p2psrv.New(
		&p2psrv.Options{
			PrivateKey:     nodeKey,
			MaxPeers:       25,
			ListenAddr:     ctx.String("port"),
			BootstrapNodes: []*discover.Node{discover.MustParseNode(boot)},
		})

	bestBlockUpdated := make(chan struct{})
	packedChan := make(chan *packedEvent)
	///////

	var goes co.Goes
	c, cancel := context.WithCancel(context.Background())

	goes.Go(func() {
		runCommunicator(c, communicator, p2pSrv)
	})
	goes.Go(func() {
		broadcastTxLoop(c, communicator, txpool)
	})
	goes.Go(func() {
		txPoolUpdateLoop(c, communicator, txpool)
	})
	goes.Go(func() {
		consentLoop(c, communicator, chain, packedChan, bestBlockUpdated, consensus)
	})
	goes.Go(func() {
		packLoop(c, communicator, chain, packedChan, bestBlockUpdated, packer, txpool, privateKey)
	})
	goes.Go(func() {
		runRestful(c, rest, lsr)
	})

	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)

	select {
	case <-interrupt:
		go func() {
			// force exit when rcvd 10 interrupts
			for i := 0; i < 10; i++ {
				<-interrupt
			}
			os.Exit(1)
		}()
		cancel()
		goes.Wait()
	}

	return nil
}

func loadNodeKey(ctx *cli.Context) (key *ecdsa.PrivateKey, err error) {
	keyFile := ctx.String("nodekey")
	if keyFile == "" {
		// no file specified, use default file path
		home, err := homeDir()
		if err != nil {
			return nil, err
		}
		keyFile = filepath.Join(home, ".thor-node.key")
	} else if !filepath.IsAbs(keyFile) {
		// resolve to absolute path
		keyFile, err = filepath.Abs(keyFile)
		if err != nil {
			return nil, err
		}
	}

	// try to load from file
	if key, err = crypto.LoadECDSA(keyFile); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		return key, nil
	}

	// no such file, generate new key and write in
	key, err = crypto.GenerateKey()
	if err != nil {
		return nil, err
	}

	if err := crypto.SaveECDSA(keyFile, key); err != nil {
		return nil, err
	}
	return key, nil
}

func loadAccount(ctx *cli.Context) (thor.Address, *ecdsa.PrivateKey, error) {
	keyString := ctx.String("key")
	if keyString != "" {
		key, err := crypto.HexToECDSA(keyString)
		if err != nil {
			return thor.Address{}, nil, err
		}
		return thor.Address(crypto.PubkeyToAddress(key.PublicKey)), key, nil
	}

	index := rand.Intn(len(genesis.Dev.Accounts()))
	return genesis.Dev.Accounts()[index].Address, genesis.Dev.Accounts()[index].PrivateKey, nil
}

func homeDir() (string, error) {
	// try to get HOME env
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}

	user, err := user.Current()
	if err != nil {
		return "", err
	}
	if user.HomeDir != "" {
		return user.HomeDir, nil
	}

	return os.Getwd()
}

func runCommunicator(ctx context.Context, communicator *comm.Communicator, p2pSrv *p2psrv.Server) {
	if err := p2pSrv.Start("thor@111111", communicator.Protocols()); err != nil {
		log.Error("%v", err)
		return
	}
	defer p2pSrv.Stop()

	peerCh := make(chan *p2psrv.Peer)
	p2pSrv.SubscribePeer(peerCh)

	communicator.Start(peerCh)
	defer communicator.Stop()

	<-ctx.Done()
}

func runRestful(ctx context.Context, rest *http.Server, lsr net.Listener) {
	go func() {
		<-ctx.Done()
		rest.Shutdown(context.TODO())
	}()

	if err := rest.Serve(lsr); err != http.ErrServerClosed {
		log.Error(fmt.Sprintf("%v", err))
	}
}
