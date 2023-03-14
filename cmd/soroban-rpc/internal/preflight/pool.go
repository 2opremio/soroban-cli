package preflight

import (
	"context"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-tools/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-tools/cmd/soroban-rpc/internal/workerpool"
)

type PreflightWorkerPool struct {
	ledgerEntryReader db.LedgerEntryReader
	networkPassphrase string
	logger            *log.Entry
	wp                *workerpool.WorkerPool[PreflightParameters, Preflight]
}

func NewPreflightWorkerPool(workerCount uint, ledgerEntryReader db.LedgerEntryReader, networkPassphrase string, logger *log.Entry) *PreflightWorkerPool {
	return &PreflightWorkerPool{
		ledgerEntryReader: ledgerEntryReader,
		networkPassphrase: networkPassphrase,
		logger:            logger,
		wp:                workerpool.NewWorkerPool[PreflightParameters, Preflight](GetPreflight, workerCount, 0),
	}
}

func (pwp *PreflightWorkerPool) Close() {
	pwp.wp.Close()
}

func (pwp *PreflightWorkerPool) GetPreflight(ctx context.Context, sourceAccount xdr.AccountId, op xdr.InvokeHostFunctionOp) (Preflight, error) {
	params := PreflightParameters{
		Logger:             pwp.logger,
		SourceAccount:      sourceAccount,
		InvokeHostFunction: op,
		NetworkPassphrase:  pwp.networkPassphrase,
		LedgerEntryReader:  pwp.ledgerEntryReader,
	}
	pwp.wp.RunJob(ctx, params)
}
