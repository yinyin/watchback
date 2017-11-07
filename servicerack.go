package watchback

import (
	"fmt"
	"time"
	"log"
	"errors"
	uatomic "go.uber.org/atomic"
)

var ErrNoFrontNodeInService = errors.New("none of front nodes in service")
var ErrCannotConcurServiceActivation = errors.New("rejected by peer node on concur service activation")
var ErrFailedOnServiceActivatingProcess = errors.New("failed on running service activating process")
var ErrTakeOverTimeout = errors.New("time-out on service take over")

var EmptyServiceControllerStandbyPeriod = time.Minute * 10

type ServiceTimingConfig struct {
	AcceptablePreparePeriod             time.Duration
	AcceptableOnServiceSelfCheckPeriod  time.Duration
	AcceptableOffServiceSelfCheckPeriod time.Duration
	AcceptableServiceActivationPeriod   time.Duration
	AcceptableServiceReleasingPeriod    time.Duration
	AcceptableControllerClosePeriod time.Duration

	AcceptableOnServiceSelfCheckFailurePeriod  time.Duration
	AcceptableOffServiceSelfCheckFailurePeriod time.Duration
	AcceptableFrontNodeEmptyPeriod             time.Duration

	OnServiceSelfCheckPeriod  time.Duration
	OffServiceSelfCheckPeriod time.Duration

	ServiceActivationFailureBlackoutPeriod time.Duration
	ServiceReleaseSuccessBlackoutPeriod    time.Duration
	ServiceReleaseFailureBlackoutPeriod    time.Duration

	BootupOperationalDelay time.Duration
}

func (c * ServiceTimingConfig) copyFrom(other * ServiceTimingConfig) {
	*c = *other
}

type ServiceRack struct {
	localNodeId int32
	serviceController ServiceControlAdapter

	frontNodes []*WorkerNode
	allNodes []*WorkerNode
	frontNodesReady bool

	availability availabilityLogic
	controlRunner callableBundleRunner
	stateTransit stateTransporter

	externalOverrideAvailable availabilityLogic
	anyFrontNodeAvailable availabilityLogic

	serviceActivating uatomic.Bool
	servicing uatomic.Bool

	lastSelfCheckAt time.Time
	lastSelfCheckResult error

	timingConfig ServiceTimingConfig
}

func newServiceRack(localNodeId int32, serviceController ServiceControlAdapter, timingConfig * ServiceTimingConfig) (serviceRack * ServiceRack) {
	serviceRack = &ServiceRack {
		localNodeId: localNodeId,
		serviceController: serviceController,
		frontNodes: make([]*WorkerNode, 0),
		allNodes: make([]*WorkerNode, 0),
		frontNodesReady: false,
		availability: newAvailabilityLogic(),
		controlRunner: newCallableBundleRunner(1),
		stateTransit: newStateTransporter(),
		externalOverrideAvailable: newAvailabilityLogic(),
		anyFrontNodeAvailable: newAvailabilityLogic(),
		lastSelfCheckResult: nil,
	}
	serviceRack.serviceActivating.Store(false)
	serviceRack.servicing.Store(false)
	serviceRack.timingConfig.copyFrom(timingConfig)
	if serviceRack.timingConfig.BootupOperationalDelay > 0 {
		serviceRack.externalOverrideAvailable.AvailableWithin(serviceRack.timingConfig.BootupOperationalDelay)
	}
	return serviceRack
}

func (x * ServiceRack) AddNode(nodeId int32, messenger NodeMessagingAdapter, messagingTimingConfig *NodeMessagingTimingConfig) (workerNode * WorkerNode, err error) {
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
	workerNode = newWorkerNode(nodeId, messenger, messagingTimingConfig)
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
	return x.durationToNextSelfCheck(x.timingConfig.OnServiceSelfCheckPeriod)
}

func (x * ServiceRack) durationToNextOffServiceSelfCheck() (result time.Duration) {
	return x.durationToNextSelfCheck(x.timingConfig.OffServiceSelfCheckPeriod)
}

func (x * ServiceRack) isFrontNodeId(nodeId int32) (found bool) {
	for _, node := range x.frontNodes {
		if node.nodeId == nodeId {
			return true
		}
	}
	return false
}

// Check if any front node is operating normally (ie: is running service)
func (x * ServiceRack) checkFrontNode() (err error) {
	for _, node := range x.frontNodes {
		onService, err := node.IsOnService()
		if nil != err {
			log.Printf("WARN: caught err on request IsOnService() on node: (id=%v)", node.nodeId)
		}
		if true == onService {
			x.anyFrontNodeAvailable.AvailableWithin(x.timingConfig.AcceptableFrontNodeEmptyPeriod)
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

// Activate service and return success state.
// This method does not check current servicing status. Must NOT invoke if service is running already.
func (x * ServiceRack) activateService(forceActivation bool) (err error) {
	if true == x.servicing.Load() {
		return nil	// already running service
	}
	x.serviceActivating.Store(true)
	defer x.serviceActivating.Store(false)
	if false == x.concurServiceActivation(forceActivation) {
		return ErrCannotConcurServiceActivation	// rejected, continue off-service checks
	}
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runServiceActivation)")
		x.servicing.Store(true)
		return nil
	}
	log.Printf("INFO: attempt to activate service (last-self-check-result: %v, force-activation: %v)", x.lastSelfCheckResult, forceActivation)
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.ActivateService, x.timingConfig.AcceptableServiceActivationPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke ActivateService: %v", err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.servicing.Store(true)
			x.availability.AvailableWithin(x.timingConfig.AcceptableOnServiceSelfCheckFailurePeriod)
			return nil
		} else {
			x.availability.BlackoutWithin(x.timingConfig.ServiceActivationFailureBlackoutPeriod)
			log.Printf("WARN: failed on activating service: %v", err)
		}
	}
	return ErrFailedOnServiceActivatingProcess	// failed service activation, continue off-service checks

}

func (x * ServiceRack) runPrepare() (err error) {
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runPrepare)")
		return nil
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.Prepare, x.timingConfig.AcceptablePreparePeriod)
	if nil != err {
		return err
	}
	defer cancel()
	err = <-c
	return err
}

func (x * ServiceRack) runClose() (err error) {
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runClose)")
		return nil
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.Close, x.timingConfig.AcceptableControllerClosePeriod)
	if nil != err {
		return err
	}
	defer cancel()
	err = <-c
	x.serviceController = nil
	return err
}

func (x * ServiceRack) runOnServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if false == x.servicing.Load() {
		return x.runOffServiceSelfCheck()
	}
	defer x.renewLastSelfCheckTimeStamp()
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runOnServiceSelfCheck)")
		return x.runOnServiceSelfCheck, x.durationToNextOnServiceSelfCheck()
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.OnServiceSelfCheck, x.timingConfig.AcceptableOnServiceSelfCheckPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke OnServiceSelfCheck: %v", err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.availability.AvailableWithin(x.timingConfig.AcceptableOnServiceSelfCheckFailurePeriod)
		}
	}
	x.lastSelfCheckResult = err
	return x.runOnServiceSelfCheck, x.durationToNextOnServiceSelfCheck()
}

func (x * ServiceRack) runOffServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if true == x.servicing.Load() {
		return x.runOnServiceSelfCheck()
	}
	defer x.renewLastSelfCheckTimeStamp()
	if x.externalOverrideAvailable.Availability() {
		log.Printf("WARN: external overrided, skip self check.")
		return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
	}
	if nil ==  x.serviceController {
		log.Printf("WARN: empty service controller (runOffServiceSelfCheck)")
		return x.runFrontNodeCheck, 0
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.OffServiceSelfCheck, x.timingConfig.AcceptableOffServiceSelfCheckPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke OffServiceSelfCheck: %v", err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.availability.AvailableWithin(x.timingConfig.AcceptableOffServiceSelfCheckFailurePeriod)
			x.lastSelfCheckResult = nil
			return x.runFrontNodeCheck, 0
		}
	}
	x.lastSelfCheckResult = err
	return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
}

func (x * ServiceRack) runFrontNodeCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if err := x.checkFrontNode(); nil != err {
		if false == x.anyFrontNodeAvailable.Availability() {
			if true == x.availability.Availability() {
				return x.runServiceActivation, 0
			}
			log.Printf("INFO: not acquire service: all front node not available but local is not available, too.")
		} else {
			log.Printf("INFO: front node check failed but still within availability range.")
		}
	}
	return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
}

func (x * ServiceRack) runServiceActivation() (nextHandler stateHandler, invokeAfter time.Duration) {
	if err := x.activateService(false); nil != err {
		log.Printf("ERR: failed service activation, continue off-service checks: %v", err)
		return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
		}
	return x.runOnServiceSelfCheck, x.durationToNextOnServiceSelfCheck()
}

func (x * ServiceRack) runServiceRelease() (nextHandler stateHandler, invokeAfter time.Duration) {
	if (false == x.servicing.Load()) || (nil ==  x.serviceController) {
		log.Printf("WARN: empty service controller (runServiceRelease)")
		x.servicing.Store(false)
		return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.ReleaseService, x.timingConfig.AcceptableServiceReleasingPeriod)
	if nil != err {
		log.Printf("ERR: failed on invoke ReleaseService: %v", err)
		x.availability.BlackoutWithin(x.timingConfig.ServiceReleaseFailureBlackoutPeriod)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.availability.BlackoutWithin(x.timingConfig.ServiceReleaseSuccessBlackoutPeriod)
		} else {
			x.availability.BlackoutWithin(x.timingConfig.ServiceReleaseFailureBlackoutPeriod)
			log.Printf("WARN: failed on releasing service: %v", err)
		}
	}
	x.servicing.Store(false)
	return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
}

func (x * ServiceRack) stopService() {
	if true == x.servicing.Load() {
		x.stateTransit.AppendDetour(func() {
			x.runServiceRelease()
		})
	}
}

// Answers service activation requests from remote peers.
// Intent to be invoke by service communication adapter
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
	x.stopService()
	return true
}

func (x * ServiceRack) ServiceTakeOver() (err error) {
	if true == x.servicing.Load() {
		log.Printf("INFO: ignoring service take over: service already running.")
		return nil
	}
	if true == x.serviceActivating.Load() {
		log.Printf("INFO: ignoring service take over: service activation in progress.")
		return nil
	}
	ch := make(chan error, 1)
	x.stateTransit.AppendDetour(func() {
		ch <- x.activateService(true)
	})
	select {
	case err := <-ch:
		return err
	}
	return ErrTakeOverTimeout
}

func (x * ServiceRack) Close() {
	x.stopService()
	x.stateTransit.AppendDetour(func() {
		x.runClose()
	})
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
	return x.stateTransit.RunStateTransportLoop(x.runOffServiceSelfCheck)
}
