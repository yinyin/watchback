package watchback

import (
	"fmt"
	"time"
	"log"
	"context"
	"errors"
)

var ErrNoFrontNodeInService = errors.New("none of front nodes in service")

type ServiceRack struct {
	localNodeId int32
	serviceController ServiceControlAdapter

	frontNodes []*WorkerNode
	allNodes []*WorkerNode
	frontNodesReady bool

	availability availabilityLogic
	controlRunner callableBundleRunner
	stateTransit stateTransporter

	lastSelfCheckResult error

	acceptablePreparePeriod time.Duration
	acceptableOffServiceSelfCheckPeriod time.Duration
	acceptableFrontNodeCheckPeriod time.Duration

	acceptableOffServiceSelfCheckFailurePeriod time.Duration

	onServiceSelfCheckPeriod time.Duration
	offServiceSelfCheckPeriod time.Duration
}

func newServiceRack(localNodeId int32, serviceController ServiceControlAdapter,
	acceptablePreparePeriod, acceptableOffServiceSelfCheckPeriod, acceptableFrontNodeCheckPeriod,
	acceptableOffServiceSelfCheckFailurePeriod,
		onServiceSelfCheckPeriod, offServiceSelfCheckPeriod time.Duration) (serviceRack * ServiceRack) {
	serviceRack = &ServiceRack {
		localNodeId: localNodeId,
		serviceController: serviceController,
		frontNodes: make([]*WorkerNode, 0),
		allNodes: make([]*WorkerNode, 0),
		frontNodesReady: false,
		availability: newAvailabilityLogic(),
		controlRunner: newCallableBundleRunner(1),
		stateTransit: newStateTransporter(),
		lastSelfCheckResult: nil,
		acceptablePreparePeriod: acceptablePreparePeriod,
		acceptableOffServiceSelfCheckPeriod: acceptableOffServiceSelfCheckPeriod,
		acceptableFrontNodeCheckPeriod: acceptableFrontNodeCheckPeriod,
		acceptableOffServiceSelfCheckFailurePeriod: acceptableOffServiceSelfCheckFailurePeriod,
		onServiceSelfCheckPeriod: onServiceSelfCheckPeriod,
		offServiceSelfCheckPeriod: offServiceSelfCheckPeriod,
	}
	return serviceRack
}

func (x * ServiceRack) AddNode(nodeId int32, messenger NodeMessagingAdapter,
	flexOnServiceCheckPeriod, expectOnServiceQueryWithin, expectMessengerCloseWithin time.Duration) (workerNode * WorkerNode, err error) {
	if nodeId == x.localNodeId {
		if x.frontNodesReady {
			return nil, fmt.Errorf("duplicated local node: id=%v", nodeId)
		}
		x.frontNodesReady = true
		return nil, nil
	}
	for _, n := range x.allNodes {
		if n.nodeId == nodeId {
			return nil, fmt.Errorf("duplicated node: id=%v", nodeId)
		}
	}
	workerNode = newWorkerNode(nodeId, messenger,
		flexOnServiceCheckPeriod, expectOnServiceQueryWithin, expectMessengerCloseWithin)
	if !x.frontNodesReady {
		x.frontNodes = append(x.frontNodes, workerNode)
	}
	x.allNodes = append(x.allNodes, workerNode)
	return workerNode, nil
}

func (x * ServiceRack) startNodeLoops() {
	for _, node := range x.allNodes {
		go node.RunMessagingLoop()
	}
}

func (x * ServiceRack) stopNodeLoops() {
	for _, node := range x.allNodes {
		node.Close()
	}
}

func (x * ServiceRack) doFrontNodeCheck(ctx context.Context) (err error) {
	for _, node := range x.frontNodes {
		onService, err := node.IsOnService()
		if nil != err {
			log.Printf("WARN: caught err on request IsOnService() on node: (id=%v)", node.nodeId)
		}
		if true == onService {
			return nil
		}
	}
	return ErrNoFrontNodeInService
}

func (x * ServiceRack) runPrepare() (err error) {
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runPrepare)")
		return nil
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.Prepare, x.acceptablePreparePeriod)
	if nil != err {
		return err
	}
	defer cancel()
	err = <-c
	return err
}

func (x * ServiceRack) runOffServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runOffServiceSelfCheck)")
		return nil, 0	// TODO: impl
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.OffServiceSelfCheck, x.acceptableOffServiceSelfCheckPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke OffServiceSelfCheck: %v", err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.availability.AvailableWithin(x.acceptableOffServiceSelfCheckFailurePeriod)
		}
	}
	x.lastSelfCheckResult = err
	return nil, 0 // TODO: impl
}

func (x * ServiceRack) runFrontNodeCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.doFrontNodeCheck, x.acceptableFrontNodeCheckPeriod)
	if nil != err {
		return nil, 0 // TODO: impl
	}
	defer cancel()
	err = <-c
	if nil != err {
		if true == x.availability.Availability() {
			return x.runConcurServiceAcquire, 0
		}
		log.Printf("INFO: all front node not available but local is not available.")
	}
	return nil, 0 // TODO: impl
}

func (x * ServiceRack) runConcurServiceAcquire() (nextHandler stateHandler, invokeAfter time.Duration) {
	// TODO: check with all nodes see if any disagreement
	return nil, 0 // TODO: impl
}

func (x * ServiceRack) Close() {
	x.stateTransit.Stop()
}

func (x * ServiceRack) StateTransitionLoop() (err error) {
	x.startNodeLoops()
	defer x.stopNodeLoops()
	go x.controlRunner.RunLoop()
	defer x.controlRunner.Close()
	if err = x.runPrepare(); nil != err {
		return err
	}

	return x.stateTransit.RunStateTransportLoop(nil) // TODO: initial handler
}