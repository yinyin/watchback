package watchback

import (
	"time"
	"log"
	"context"
	"errors"
)

const maxNodeMessagingOperationAttempt = 2

var ErrExceedMaxWorkerNodeMessagingOperationAttempt = errors.New("exceed max node messaging operation attempt")

type WorkerNode struct {
	nodeId int32
	messenger NodeMessagingAdapter

	serviceOn availabilityLogic
	messagingRunner callableBundleRunner

	flexOnServiceCheckPeriod   time.Duration
	expectOnServiceQueryWithin time.Duration
	expectMessengerCloseWithin time.Duration
}

func newWorkerNode(nodeId int32, messenger NodeMessagingAdapter,
	flexOnServiceCheckPeriod, expectOnServiceQueryWithin, expectMessengerCloseWithin time.Duration) (workerNode * WorkerNode) {
	return &WorkerNode {
		nodeId:                     nodeId,
		messenger:                  messenger,
		serviceOn:                  newAvailabilityLogic(),
		messagingRunner:            newCallableBundleRunner(1),
		flexOnServiceCheckPeriod:   flexOnServiceCheckPeriod,
		expectOnServiceQueryWithin: expectOnServiceQueryWithin,
		expectMessengerCloseWithin: expectMessengerCloseWithin,
	}
}

func (n * WorkerNode) NodeId() (nodeId int32) {
	return n.nodeId
}

func (n * WorkerNode) Close() {
	c, cancel, err := n.messagingRunner.AddCallableWithTimeout(n.messenger.Close, n.expectMessengerCloseWithin)
	if nil != err {
		log.Printf("WARN: cannot invoke Close() of messenger (nodeId=%v): %v", n.nodeId, err)
	} else {
		defer cancel()
		<-c
		n.messagingRunner.Close()
	}
}

func (n * WorkerNode) RunMessagingLoop() {
	n.messagingRunner.RunLoop()
}

func (n * WorkerNode) invokeMessagingOperation(operationName string, callable callableFunc, timeout time.Duration) (err error) {
	for attempt := 0; attempt < maxNodeMessagingOperationAttempt; attempt++ {
		c, cancel, err := n.messagingRunner.AddCallableWithTimeout(callable, timeout)
		if nil != err {
			log.Printf("WARN: failed on invoke messaging operation %v (attempt=%v): %v", operationName, attempt, err)
		} else {
			defer cancel()
			err = <-c
			if nil != err {
				log.Printf("WARN: result into error on messaging operation %v (attempt=%v): %v", operationName, attempt, err)
				n.messenger.HasMessagingFailure(err)
			} else {
				return nil
			}
		}
	}
	return ErrExceedMaxWorkerNodeMessagingOperationAttempt
}

func (n * WorkerNode) requestIsOnServiceCheck(ctx context.Context) (err error) {
	onService, err := n.messenger.IsOnService(ctx)
	if nil != err {
		return err
	}
	if onService {
		n.serviceOn.AvailableWithin(n.flexOnServiceCheckPeriod)
	} else {
		select {
		case <-ctx.Done():
		default:
			n.serviceOn.Reset()	// reset on context not expire yet
		}
	}
	return nil
}

func (n * WorkerNode) IsOnService() (running bool, err error) {
	err = n.invokeMessagingOperation("IsOnService", n.requestIsOnServiceCheck, n.expectOnServiceQueryWithin)
	return n.serviceOn.Availability(), err
}
