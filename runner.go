package watchback

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// ErrBundleQueueFulled is raised when callable bundle queue is fulled and not able to complete requested operation
var ErrBundleQueueFulled = errors.New("callable bundle queue is fulled")

// ErrCallableTimeout is raised when callable not able to complete before context timeout
var ErrCallableTimeout = errors.New("context of callable done before callable complete")

// ExpiredCallableResultCollectPeriod is a predefined period to collect result of callable on callable operation timeout
const ExpiredCallableResultCollectPeriod = time.Second * 3

type callableFunc func(ctx context.Context) (err error)

type callableBundle struct {
	callable    callableFunc
	ctx         context.Context
	resultStore chan error
}

func newCallableBundle(callable callableFunc, ctx context.Context) (bundle *callableBundle) {
	return &callableBundle{
		callable:    callable,
		ctx:         ctx,
		resultStore: make(chan error, 1),
	}
}

func (b *callableBundle) setErrorState(err error, ok bool) {
	if nil != err {
		log.Printf("invoked bundle %v completed but returns an error: %v.", b, err)
		b.resultStore <- err
	} else if !ok {
		log.Printf("invoked bundle %v completed but failed to fetch error state from channel.", b)
		b.resultStore <- fmt.Errorf("failed on fetch error state for %v", b)
	} else {
		b.resultStore <- nil
	}
	close(b.resultStore)
}

type callableBundleRunner chan *callableBundle

func newCallableBundleRunner(bufferSize int) (runner callableBundleRunner) {
	return make(chan *callableBundle, bufferSize)
}

func (r callableBundleRunner) runCallable(ch chan<- error, bundle *callableBundle) {
	err := bundle.callable(bundle.ctx)
	ch <- err
	close(ch)
}

func (r callableBundleRunner) AddCallableWithContext(callable callableFunc, ctx context.Context) (retCh <-chan error, err error) {
	bundle := newCallableBundle(callable, ctx)
	select {
	case r <- bundle:
		return bundle.resultStore, nil
	default:
		return nil, ErrBundleQueueFulled
	}
}

func (r callableBundleRunner) AddCallableWithTimeout(callable callableFunc, timeout time.Duration) (retCh <-chan error, cancel context.CancelFunc, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if retCh, err = r.AddCallableWithContext(callable, ctx); nil != err {
		cancel()
		cancel = nil
	}
	return retCh, cancel, err
}

func (r callableBundleRunner) Close() {
	close(r)
}

func (r callableBundleRunner) RunLoop() {
	for bundle := range r {
		ch := make(chan error, 1)
		go r.runCallable(ch, bundle)
		select {
		case err, ok := <-ch:
			bundle.setErrorState(err, ok)
		case <-bundle.ctx.Done():
			log.Printf("context of bundle %v declares done.", bundle)
			select {
			case err, ok := <-ch:
				bundle.setErrorState(err, ok)
			case <-time.After(ExpiredCallableResultCollectPeriod):
				bundle.setErrorState(ErrCallableTimeout, true)
			}
		}
	}
}
