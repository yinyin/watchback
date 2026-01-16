package watchback

import (
	"errors"
	"sync/atomic"
	"time"
)

const detourFunctionQueueSize = 1

var ErrTransporterNotRun = errors.New("transporter is not running")
var ErrTransporterAlreadyRunning = errors.New("transporter already running")
var ErrEmptyInitialStateHandler = errors.New("initial state handler cannot be nil")

type stateHandler func() (nextHandler stateHandler, invokeAfter time.Duration)
type detourFunc func()

type stateTransporter struct {
	runningFlag  int32
	stoppingFlag int32
	detours      chan detourFunc
}

func newStateTransporter() (t stateTransporter) {
	return stateTransporter{
		runningFlag:  0,
		stoppingFlag: 0,
		detours:      nil,
	}
}

func (t *stateTransporter) running() (r bool) {
	runningFlag := atomic.LoadInt32(&t.runningFlag)
	stoppingFlag := atomic.LoadInt32(&t.stoppingFlag)
	if (t.detours != nil) && (runningFlag == 1) && (stoppingFlag == 0) {
		return true
	}
	return false
}

func (t *stateTransporter) AppendDetour(f detourFunc) (err error) {
	if !t.running() {
		return ErrTransporterNotRun
	}
	t.detours <- f
	return nil
}

func (t *stateTransporter) Stop() {
	if !t.running() {
		return
	}
	atomic.StoreInt32(&t.stoppingFlag, 1)
	t.detours <- nil
}

func (t *stateTransporter) loopStartCheck(handler stateHandler) (err error) {
	if t.running() {
		return ErrTransporterAlreadyRunning
	}
	if nil == handler {
		return ErrEmptyInitialStateHandler
	}
	return nil
}

func (t *stateTransporter) loopInit() {
	atomic.StoreInt32(&t.stoppingFlag, 0)
	t.detours = make(chan detourFunc, detourFunctionQueueSize)
	atomic.StoreInt32(&t.runningFlag, 1)
}

func (t *stateTransporter) loopStopping() {
	atomic.StoreInt32(&t.stoppingFlag, 1)
	t.detours = nil
	atomic.StoreInt32(&t.runningFlag, 0)
	atomic.StoreInt32(&t.stoppingFlag, 0)
}

func (t *stateTransporter) RunStateTransportLoop(handler stateHandler) (err error) {
	if err = t.loopStartCheck(handler); nil != err {
		return err
	}
	t.loopInit()
	defer t.loopStopping()
	// transition loop
	handler, invokeAfter := handler()
	for nil != handler {
		if invokeAfter > 0 {
			timer := time.NewTimer(invokeAfter)
			select {
			case <-timer.C:
				if !timer.Stop() {
					<-timer.C
				}
			case f := <-t.detours:
				if !timer.Stop() {
					<-timer.C
				}
				if nil == f {
					return nil
				}
				f()
			}
		}
		handler, invokeAfter = handler()
	}
	return nil
}
