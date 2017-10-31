package watchback

import (
	"fmt"
	"time"
	"log"
	"context"
	"errors"
	uatomic "go.uber.org/atomic"
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

	serviceActivating uatomic.Bool
	servicing uatomic.Bool

	lastSelfCheckAt time.Time
	lastSelfCheckResult error

	acceptablePreparePeriod time.Duration
	acceptableOnServiceSelfCheckPeriod time.Duration
	acceptableOffServiceSelfCheckPeriod time.Duration
	acceptableFrontNodeCheckPeriod time.Duration
	acceptableServiceActivationPeriod time.Duration

	acceptableOnServiceSelfCheckFailurePeriod time.Duration
	acceptableOffServiceSelfCheckFailurePeriod time.Duration

	onServiceSelfCheckPeriod time.Duration
	offServiceSelfCheckPeriod time.Duration
	serviceActivationFailureBlackoutPeriod time.Duration
}

func newServiceRack(localNodeId int32, serviceController ServiceControlAdapter,
	acceptablePreparePeriod, acceptableOnServiceSelfCheckPeriod, acceptableOffServiceSelfCheckPeriod, acceptableFrontNodeCheckPeriod, acceptableServiceActivationPeriod,
	acceptableOnServiceSelfCheckFailurePeriod, acceptableOffServiceSelfCheckFailurePeriod,
		onServiceSelfCheckPeriod, offServiceSelfCheckPeriod, serviceActivationFailureBlackoutPeriod time.Duration) (serviceRack * ServiceRack) {
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
		acceptableOnServiceSelfCheckPeriod: acceptableOnServiceSelfCheckPeriod,
		acceptableOffServiceSelfCheckPeriod: acceptableOffServiceSelfCheckPeriod,
		acceptableFrontNodeCheckPeriod: acceptableFrontNodeCheckPeriod,
		acceptableServiceActivationPeriod: acceptableServiceActivationPeriod,
		acceptableOnServiceSelfCheckFailurePeriod: acceptableOnServiceSelfCheckFailurePeriod,
		acceptableOffServiceSelfCheckFailurePeriod: acceptableOffServiceSelfCheckFailurePeriod,
		onServiceSelfCheckPeriod: onServiceSelfCheckPeriod,
		offServiceSelfCheckPeriod: offServiceSelfCheckPeriod,
		serviceActivationFailureBlackoutPeriod: serviceActivationFailureBlackoutPeriod,
	}
	serviceRack.serviceActivating.Store(false)
	serviceRack.servicing.Store(false)
	return serviceRack
}

func (x * ServiceRack) AddNode(nodeId int32, messenger NodeMessagingAdapter,
	flexOnServiceCheckPeriod, expectOnServiceQueryWithin, expectServiceActivationRequestWithin, expectMessengerCloseWithin time.Duration) (workerNode * WorkerNode, err error) {
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
		flexOnServiceCheckPeriod, expectOnServiceQueryWithin, expectServiceActivationRequestWithin, expectMessengerCloseWithin)
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

func (x * ServiceRack) renewLastSelfCheckTimeStamp() {
	x.lastSelfCheckAt = time.Now()
}

func (x * ServiceRack) durationToNextSelfCheck(d time.Duration) (result time.Duration) {
	nextCheckAt := x.lastSelfCheckAt.Add(d)
	result = nextCheckAt.Sub(time.Now())
	if result < 0 {
		result = 0
	}
	return result
}

func (x * ServiceRack) durationToNextOnServiceSelfCheck() (result time.Duration) {
	return x.durationToNextSelfCheck(x.onServiceSelfCheckPeriod)
}

func (x * ServiceRack) durationToNextOffServiceSelfCheck() (result time.Duration) {
	return x.durationToNextSelfCheck(x.offServiceSelfCheckPeriod)
}

func (x * ServiceRack) isFrontNodeId(nodeId int32) (found bool) {
	for _, node := range x.frontNodes {
		if node.nodeId == nodeId {
			return true
		}
	}
	return false
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

// Check with all nodes see if any disagreement on service activation
func (x * ServiceRack) concurServiceActivation(forceActivation bool) (success bool) {
	for _, node := range x.allNodes {
		if isApproval, err := node.RequestServiceActivationApproval(x.localNodeId, forceActivation); nil != err {
			log.Printf("WARN: failed on requesting service activiation approval from node %v: %v", node.nodeId, err)
		} else if false == isApproval {
			log.Printf("INFO: service activation request rejected by node %v", node.nodeId)
			return false
		}
	}
	return true
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

func (x * ServiceRack) runOnServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if false == x.servicing.Load() {
		return x.runOffServiceSelfCheck()
	}
	defer x.renewLastSelfCheckTimeStamp()
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runOnServiceSelfCheck)")
		return nil, 0	// TODO: impl
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.OnServiceSelfCheck, x.acceptableOnServiceSelfCheckPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke OnServiceSelfCheck: %v", err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.availability.AvailableWithin(x.acceptableOnServiceSelfCheckFailurePeriod)
		}
	}
	x.lastSelfCheckResult = err
	return nil, 0 // TODO: impl
}

func (x * ServiceRack) runOffServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	defer x.renewLastSelfCheckTimeStamp()
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
			return x.runServiceActivation, 0
		}
		log.Printf("INFO: not acquire service: all front node not available but local is not available, too.")
	}
	return nil, 0 // TODO: impl
}

func (x * ServiceRack) runServiceActivation() (nextHandler stateHandler, invokeAfter time.Duration) {
	x.serviceActivating.Store(true)
	defer x.serviceActivating.Store(false)
	if false == x.concurServiceActivation(false) {
		return nil, 0 // TODO: impl, get rejected
	}
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runServiceActivation)")
		return nil, 0	// TODO: impl
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.ActivateService, x.acceptableServiceActivationPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke ActivateService: %v", err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.servicing.Store(true)
			x.availability.AvailableWithin(x.acceptableOnServiceSelfCheckFailurePeriod)
			return x.runOnServiceSelfCheck, x.durationToNextOnServiceSelfCheck()
		} else {
			x.availability.BlackoutWithin(x.serviceActivationFailureBlackoutPeriod)
			log.Printf("WARN: failed on activating service: %v", err)
		}
	}
	return nil, 0 // TODO: impl
}

func (x * ServiceRack) RequestServiceActivationApproval(nodeId int32, forceActivation bool) (accept bool) {
	if true == x.serviceActivating.Load() {
		return false
	}
	if (false == forceActivation) && (true == x.availability.Availability()) {
		if true == x.servicing.Load() {
			return false
		}
		if false == x.isFrontNodeId(nodeId) {
			return false
		}
	}
	// TODO: release service
	return true
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
