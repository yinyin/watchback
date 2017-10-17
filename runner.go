package watchback

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

var ErrBundleQueueFulled = errors.New("callable bundle queue is fulled")
var ErrCallableTimeout = errors.New("context of callable done before callable complete")

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
	} else if true != ok {
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
			case <-time.After(time.Second * 3):
				bundle.setErrorState(ErrCallableTimeout, true)
			}
		}
	}
}
