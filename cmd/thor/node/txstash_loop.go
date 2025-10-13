// Copyright (c) 2025 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package node

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb"
)

func (n *Node) txStashLoop(ctx context.Context) {
	logger.Debug("enter tx stash loop")
	defer logger.Debug("leave tx stash loop")

	db, err := leveldb.OpenFile(n.txStashPath, nil)
	if err != nil {
		logger.Error("create tx stash", "err", err)
		return
	}
	defer db.Close()

	stash := newTxStash(db, 1000)

	{
		txs := stash.LoadAll()
		n.txPool.Fill(txs)
		logger.Debug("loaded txs from stash", "count", len(txs))
	}

	for {
		select {
		case <-ctx.Done():
			logger.Debug("received context done signal")
			return
		case txEv := <-n.txCh:
			logger.Debug("received future block signal")
			// skip executables
			if txEv.Executable != nil && *txEv.Executable {
				continue
			}
			// only stash non-executable txs
			if err := stash.Save(txEv.Tx); err != nil {
				logger.Warn("stash tx", "id", txEv.Tx.ID(), "err", err)
			} else {
				logger.Trace("stashed tx", "id", txEv.Tx.ID())
			}
		}
	}
}
