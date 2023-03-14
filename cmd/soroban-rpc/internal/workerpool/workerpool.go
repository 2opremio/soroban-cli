package workerpool

import (
	"context"
	"errors"
	"sync"
)

type workerResult[R any] struct {
	result R
	err    error
}

type workerRequest[P any, R any] struct {
	ctx        context.Context
	params     P
	resultChan chan<- workerResult[R]
}

type WorkFunc[P any, R any] func(context.Context, P) (R, error)

type WorkerPool[P any, R any] struct {
	workFunc    WorkFunc[P, R]
	done        chan struct{}
	requestChan chan workerRequest[P, R]
	wg          sync.WaitGroup
}

func NewWorkerPool[P any, R any](f WorkFunc[P, R], workerCount uint, requestBufferSize uint) *WorkerPool[P, R] {
	wp := WorkerPool[P, R]{
		workFunc:    f,
		done:        make(chan struct{}),
		requestChan: make(chan workerRequest[P, R], requestBufferSize),
	}
	for i := uint(0); i < workerCount; i++ {
		wp.wg.Add(1)
		go wp.runWorker()
	}
	return &wp
}

func (wp *WorkerPool[P, R]) runWorker() {
	defer wp.wg.Done()
	for {
		select {
		case <-wp.done:
			return
		case request := <-wp.requestChan:
			r, err := wp.workFunc(request.ctx, request.params)
			request.resultChan <- workerResult[R]{r, err}
		}
	}
}

func (wp *WorkerPool[P, R]) Close() {
	close(wp.done)
	wp.wg.Wait()
}

func (wp *WorkerPool[P, R]) RunJob(ctx context.Context, params P) (R, error) {
	resultC := make(chan workerResult[R])
	select {
	case <-wp.done:
		return *new(R), errors.New("preflight worker workerpool is closed")
	case wp.requestChan <- workerRequest[P, R]{ctx, params, resultC}:
		result := <-resultC
		return result.result, result.err
	case <-ctx.Done():
		return *new(R), ctx.Err()
	}
}
