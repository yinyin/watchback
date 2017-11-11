package watchback

import (
	"errors"
	"fmt"
	"log"
	"time"

	uatomic "go.uber.org/atomic"
)

var ErrNoFrontNodeInService = errors.New("none of front nodes in service")
var ErrCannotConcurServiceActivation = errors.New("rejected by peer node on concur service activation")
var ErrFailedOnServiceActivatingProcess = errors.New("failed on running service activating process")

type ServiceTimingConfig struct {
	AcceptablePreparePeriod             time.Duration
	AcceptableOnServiceSelfCheckPeriod  time.Duration
	AcceptableOffServiceSelfCheckPeriod time.Duration
	AcceptableServiceActivationPeriod   time.Duration
	AcceptableServiceReleasingPeriod    time.Duration
	AcceptableControllerClosePeriod     time.Duration

	AcceptableOnServiceSelfCheckFailurePeriod  time.Duration
	AcceptableOffServiceSelfCheckFailurePeriod time.Duration
	AcceptableFrontNodeEmptyPeriod             time.Duration

	OnServiceSelfCheckPeriod  time.Duration
	OffServiceSelfCheckPeriod time.Duration

	ServiceActivationSuccessExemptionPeriod time.Duration
	ServiceActivationFailureBlackoutPeriod  time.Duration
	ServiceReleaseSuccessBlackoutPeriod     time.Duration
	ServiceReleaseFailureBlackoutPeriod     time.Duration

	BootupOperationalDelay time.Duration
}

func (c *ServiceTimingConfig) copyFrom(other *ServiceTimingConfig) {
	*c = *other
}

const AdviceAcceptablePreparePeriodTooShort = "Acceptable prepare operation period might too short (< 1ms)"
const AdviceAcceptableOnServiceSelfCheckPeriodTooShort = "Acceptable on-service self check operation period might too short (< 1ms)"
const AdviceAcceptableOffServiceSelfCheckPeriodTooShort = "Acceptable off-service self check period might too short (< 1ms)"
const AdviceAcceptableServiceActivationPeriodTooShort = "Acceptable service activating operation period might too short (< 1ms)"
const AdviceAcceptableServiceReleasingPeriodTooShort = "Acceptable service releasing operation period might too short (< 1ms)"
const AdviceAcceptableControllerClosePeriodTooShort = "Acceptable controller closing operation period might too short (< 1ns)"
const AdviceAcceptableOnServiceSelfCheckFailurePeriodShorterThanOperation = "Acceptable on-service failure period shorter than acceptable operation time"
const AdviceAcceptableOffServiceSelfCheckFailurePeriodShorterThanOperation = "Acceptable off-service failure period shorter than acceptable operation time"
const AdviceOnServiceSelfCheckPeriodShorterThanOperation = "On-service self check period shorter than check operation might need"
const AdviceOffServiceSelfCheckPeriodShorterThanOperation = "Off-service self check period shorter than check operation might need"
const AdviceServiceActivationSuccessExemptionPeriodShorterThanCheck = "Exemption period of successful service activation shorter than self check period"
const AdviceServiceActivationFailureBlackoutPeriodShorterThanCheckAndActivateOperation = "Blackout period of failed service activation shorter than sum of service check period and service activation time"
const AdviceServiceReleaseSuccessBlackoutPeriodShorterThanCheckAndActivateOperation = "Blackout period for successful service releasing shorter than sum of service check period and service activation time"
const AdviceServiceReleaseFailureBlackoutPeriodShorterThanCheckAndActivateOperation = "Blackout period for failed service releasing shorter than sum of service check period and service activation time"

func (c *ServiceTimingConfig) Advice() (advices []string) {
	advices = make([]string, 0)

	if c.AcceptablePreparePeriod < time.Millisecond {
		advices = append(advices, AdviceAcceptablePreparePeriodTooShort)
	}
	if c.AcceptableOnServiceSelfCheckPeriod < time.Millisecond {
		advices = append(advices, AdviceAcceptableOnServiceSelfCheckPeriodTooShort)
	}
	if c.AcceptableOffServiceSelfCheckPeriod < time.Millisecond {
		advices = append(advices, AdviceAcceptableOffServiceSelfCheckPeriodTooShort)
	}
	if c.AcceptableServiceActivationPeriod < time.Millisecond {
		advices = append(advices, AdviceAcceptableServiceActivationPeriodTooShort)
	}
	if c.AcceptableServiceReleasingPeriod < time.Millisecond {
		advices = append(advices, AdviceAcceptableServiceReleasingPeriodTooShort)
	}
	if c.AcceptableControllerClosePeriod < time.Nanosecond {
		advices = append(advices, AdviceAcceptableControllerClosePeriodTooShort)
	}
	if c.AcceptableOnServiceSelfCheckFailurePeriod < c.AcceptableOnServiceSelfCheckPeriod {
		advices = append(advices, AdviceAcceptableOnServiceSelfCheckFailurePeriodShorterThanOperation)
	}
	if c.AcceptableOffServiceSelfCheckFailurePeriod < c.AcceptableOffServiceSelfCheckPeriod {
		advices = append(advices, AdviceAcceptableOffServiceSelfCheckFailurePeriodShorterThanOperation)
	}
	if c.OnServiceSelfCheckPeriod < c.AcceptableOnServiceSelfCheckPeriod {
		advices = append(advices, AdviceOnServiceSelfCheckPeriodShorterThanOperation)
	}
	if c.OffServiceSelfCheckPeriod < c.AcceptableOffServiceSelfCheckPeriod {
		advices = append(advices, AdviceOffServiceSelfCheckPeriodShorterThanOperation)
	}
	var largerSelfCheckPeriod time.Duration
	if c.OnServiceSelfCheckPeriod > c.OffServiceSelfCheckPeriod {
		largerSelfCheckPeriod = c.OnServiceSelfCheckPeriod
	} else {
		largerSelfCheckPeriod = c.OffServiceSelfCheckPeriod
	}
	if c.ServiceActivationSuccessExemptionPeriod < largerSelfCheckPeriod {
		advices = append(advices, AdviceServiceActivationSuccessExemptionPeriodShorterThanCheck)
	}
	expectServiceActivationCycleTime := largerSelfCheckPeriod + c.AcceptableServiceActivationPeriod
	if c.ServiceActivationFailureBlackoutPeriod < expectServiceActivationCycleTime {
		advices = append(advices, AdviceServiceActivationFailureBlackoutPeriodShorterThanCheckAndActivateOperation)
	}
	if c.ServiceReleaseSuccessBlackoutPeriod < expectServiceActivationCycleTime {
		advices = append(advices, AdviceServiceReleaseSuccessBlackoutPeriodShorterThanCheckAndActivateOperation)
	}
	if c.ServiceReleaseFailureBlackoutPeriod < expectServiceActivationCycleTime {
		advices = append(advices, AdviceServiceReleaseFailureBlackoutPeriodShorterThanCheckAndActivateOperation)
	}
	return advices
}

type ServiceRack struct {
	serviceId int32

	localNodeId       int32
	serviceController ServiceControlAdapter

	frontNodes      []*WorkerNode
	allNodes        []*WorkerNode
	frontNodesReady bool

	availability  availabilityLogic
	controlRunner callableBundleRunner
	stateTransit  stateTransporter

	externalOverrideAvailable availabilityLogic
	anyFrontNodeAvailable     availabilityLogic

	serviceActivating uatomic.Bool
	servicing         uatomic.Bool

	lastSelfCheckAt     time.Time
	lastSelfCheckResult error

	timingConfig ServiceTimingConfig
}

func NewServiceRack(serviceId int32, localNodeId int32, serviceController ServiceControlAdapter, timingConfig *ServiceTimingConfig) (serviceRack *ServiceRack) {
	serviceRack = &ServiceRack{
		serviceId:                 serviceId,
		localNodeId:               localNodeId,
		serviceController:         serviceController,
		frontNodes:                make([]*WorkerNode, 0),
		allNodes:                  make([]*WorkerNode, 0),
		frontNodesReady:           false,
		availability:              newAvailabilityLogic(),
		controlRunner:             newCallableBundleRunner(1),
		stateTransit:              newStateTransporter(),
		externalOverrideAvailable: newAvailabilityLogic(),
		anyFrontNodeAvailable:     newAvailabilityLogic(),
		lastSelfCheckResult:       nil,
	}
	serviceRack.serviceActivating.Store(false)
	serviceRack.servicing.Store(false)
	serviceRack.timingConfig.copyFrom(timingConfig)
	if serviceRack.timingConfig.BootupOperationalDelay > 0 {
		serviceRack.externalOverrideAvailable.AvailableWithin(serviceRack.timingConfig.BootupOperationalDelay)
	}
	return serviceRack
}

func (x *ServiceRack) AddNode(nodeId int32, messenger NodeMessagingAdapter, messagingTimingConfig *NodeMessagingTimingConfig) (workerNode *WorkerNode, err error) {
	if nodeId == x.localNodeId {
		if x.frontNodesReady {
			return nil, fmt.Errorf("[service-id: %d] duplicated local node: id=%v", x.serviceId, nodeId)
		}
		x.frontNodesReady = true
		return nil, nil
	}
	for _, n := range x.allNodes {
		if n.nodeId == nodeId {
			return nil, fmt.Errorf("[service-id: %d] duplicated node: id=%v", x.serviceId, nodeId)
		}
	}
	workerNode = newWorkerNode(nodeId, messenger, messagingTimingConfig)
	if !x.frontNodesReady {
		x.frontNodes = append(x.frontNodes, workerNode)
	}
	x.allNodes = append(x.allNodes, workerNode)
	return workerNode, nil
}

func (x *ServiceRack) startNodeLoops() {
	for _, node := range x.allNodes {
		go node.RunMessagingLoop()
	}
}

func (x *ServiceRack) stopNodeLoops() {
	for _, node := range x.allNodes {
		node.Close()
	}
}

func (x *ServiceRack) renewLastSelfCheckTimeStamp() {
	x.lastSelfCheckAt = time.Now()
}

func (x *ServiceRack) durationToNextSelfCheck(d time.Duration) (result time.Duration) {
	nextCheckAt := x.lastSelfCheckAt.Add(d)
	result = nextCheckAt.Sub(time.Now())
	if result < 0 {
		result = 0
	}
	return result
}

func (x *ServiceRack) durationToNextOnServiceSelfCheck() (result time.Duration) {
	return x.durationToNextSelfCheck(x.timingConfig.OnServiceSelfCheckPeriod)
}

func (x *ServiceRack) durationToNextOffServiceSelfCheck() (result time.Duration) {
	return x.durationToNextSelfCheck(x.timingConfig.OffServiceSelfCheckPeriod)
}

func (x *ServiceRack) isFrontNodeId(nodeId int32) (found bool) {
	for _, node := range x.frontNodes {
		if node.nodeId == nodeId {
			return true
		}
	}
	return false
}

// Check if any front node is operating normally (ie: is running service)
func (x *ServiceRack) checkFrontNode() (err error) {
	for _, node := range x.frontNodes {
		onService, err := node.IsOnService()
		if nil != err {
			log.Printf("WARN: [service-id: %d] caught err on request IsOnService() on node: (id=%v)", x.serviceId, node.nodeId)
		}
		if true == onService {
			x.anyFrontNodeAvailable.AvailableWithin(x.timingConfig.AcceptableFrontNodeEmptyPeriod)
			return nil
		} else {
			log.Printf("INFO: [service-id: %d] peer node off-service: (id=%v)", x.serviceId, node.nodeId)
		}
	}
	log.Printf("WARN: [service-id: %d] no front node in service", x.serviceId)
	return ErrNoFrontNodeInService
}

// Check with all nodes see if any disagreement on service activation
func (x *ServiceRack) concurServiceActivation(forceActivation bool) (success bool) {
	for _, node := range x.allNodes {
		if isApproval, err := node.RequestServiceActivationApproval(x.localNodeId, forceActivation); nil != err {
			log.Printf("WARN: [service-id: %d] failed on requesting service activiation approval from node %v: %v", x.serviceId, node.nodeId, err)
		} else if false == isApproval {
			log.Printf("INFO: [service-id: %d] service activation request rejected by node %v", x.serviceId, node.nodeId)
			return false
		} else {
			log.Printf("INFO: [service-id: %d] service activation request agreed by node %v", x.serviceId, node.nodeId)
		}
	}
	return true
}

// Activate service and return success state.
func (x *ServiceRack) activateService(forceActivation bool) (err error) {
	if true == x.servicing.Load() {
		log.Printf("INFO: [service-id: %d] ignoring service activation: service already running (force=%v).", x.serviceId, forceActivation)
		return nil
	}
	if true == x.serviceActivating.Load() {
		log.Printf("INFO: [service-id: %d] ignoring service activation: service activation in progress (force=%v).", x.serviceId, forceActivation)
		return nil
	}
	x.serviceActivating.Store(true)
	defer x.serviceActivating.Store(false)
	if false == x.concurServiceActivation(forceActivation) {
		log.Printf("WARN: [service-id: %d] failed on concur service activation with peer nodes", x.serviceId)
		return ErrCannotConcurServiceActivation // rejected, continue off-service checks
	}
	if nil == x.serviceController {
		log.Printf("WARN: [service-id: %d] empty service controller (runServiceActivation)", x.serviceId)
		x.servicing.Store(true)
		return nil
	}
	log.Printf("INFO: [service-id: %d] attempt to activate service (last-self-check-result: %v, force-activation: %v)", x.serviceId, x.lastSelfCheckResult, forceActivation)
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.ActivateService, x.timingConfig.AcceptableServiceActivationPeriod)
	if nil != err {
		log.Printf("ERR: [service-id: %d] failed on invoke ActivateService: %v", x.serviceId, err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			x.servicing.Store(true)
			x.availability.ResetWithAvailableWithin(x.timingConfig.ServiceActivationSuccessExemptionPeriod)
			log.Printf("INFO: [service-id: %d] activated service", x.serviceId)
			return nil
		} else {
			x.availability.BlackoutWithin(x.timingConfig.ServiceActivationFailureBlackoutPeriod)
			log.Printf("WARN: [service-id: %d] failed on activating service: %v", x.serviceId, err)
		}
	}
	return ErrFailedOnServiceActivatingProcess // failed service activation, continue off-service checks

}

func (x *ServiceRack) runPrepare() (err error) {
	if nil == x.serviceController {
		log.Printf("WARN: [service-id: %d] empty service controller (runPrepare)", x.serviceId)
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

func (x *ServiceRack) runClose() (err error) {
	if nil == x.serviceController {
		log.Printf("WARN: [service-id: %d] empty service controller (runClose)", x.serviceId)
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

func (x *ServiceRack) runOnServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if false == x.servicing.Load() {
		log.Printf("INFO: [service-id: %d] redirect from OnServiceSelfCheck to OffServiceSelfCheck", x.serviceId)
		return x.runOffServiceSelfCheck()
	}
	x.renewLastSelfCheckTimeStamp()
	if nil == x.serviceController {
		log.Printf("WARN: [service-id: %d] empty service controller (runOnServiceSelfCheck)", x.serviceId)
		return x.runOnServiceSelfCheck, x.durationToNextOnServiceSelfCheck()
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.OnServiceSelfCheck, x.timingConfig.AcceptableOnServiceSelfCheckPeriod)
	if nil != err {
		log.Printf("ERR: [service-id: %d] failed on invoke OnServiceSelfCheck: %v", x.serviceId, err)
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

func (x *ServiceRack) runOffServiceSelfCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if true == x.servicing.Load() {
		log.Printf("INFO: [service-id: %d] redirect from OffServiceSelfCheck to OnServiceSelfCheck", x.serviceId)
		return x.runOnServiceSelfCheck()
	}
	x.renewLastSelfCheckTimeStamp()
	if x.externalOverrideAvailable.Availability() {
		log.Printf("WARN: [service-id: %d] external overrided, skip self check.", x.serviceId)
		return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
	}
	if nil == x.serviceController {
		log.Printf("WARN: [service-id: %d] empty service controller (runOffServiceSelfCheck)", x.serviceId)
		return x.runFrontNodeCheck, 0
	}
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.OffServiceSelfCheck, x.timingConfig.AcceptableOffServiceSelfCheckPeriod)
	if nil != err {
		log.Printf("ERR: [service-id: %d] failed on invoke OffServiceSelfCheck: %v", x.serviceId, err)
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

func (x *ServiceRack) runFrontNodeCheck() (nextHandler stateHandler, invokeAfter time.Duration) {
	if err := x.checkFrontNode(); nil != err {
		if false == x.anyFrontNodeAvailable.Availability() {
			if true == x.availability.Availability() {
				log.Printf("INFO: [service-id: %d] attempt to activate service: none of front node available.", x.serviceId)
				return x.runServiceActivation, 0
			}
			log.Printf("INFO: [service-id: %d] not acquire service: all front node not available but local is not available, too.", x.serviceId)
		} else {
			log.Printf("INFO: [service-id: %d] front node check failed but still within availability range.", x.serviceId)
		}
	}
	return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
}

func (x *ServiceRack) runServiceActivation() (nextHandler stateHandler, invokeAfter time.Duration) {
	if err := x.activateService(false); nil != err {
		log.Printf("ERR: [service-id: %d] failed service activation, continue off-service checks: %v", x.serviceId, err)
		return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
	}
	return x.runOnServiceSelfCheck, x.durationToNextOnServiceSelfCheck()
}

func (x *ServiceRack) runServiceRelease() (nextHandler stateHandler, invokeAfter time.Duration) {
	if serviceRunning := x.servicing.Load(); (false == serviceRunning) || (nil == x.serviceController) {
		if nil == x.serviceController {
			log.Printf("WARN: [service-id: %d] empty service controller (runServiceRelease)", x.serviceId)
		}
		if false == serviceRunning {
			log.Printf("INFO: [service-id: %d] service is not running (runServiceRelease)", x.serviceId)
		}
		x.servicing.Store(false)
		return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
	}
	log.Printf("INFO: [service-id: %d] going to release service", x.serviceId)
	c, cancel, err := x.controlRunner.AddCallableWithTimeout(x.serviceController.ReleaseService, x.timingConfig.AcceptableServiceReleasingPeriod)
	if nil != err {
		log.Printf("ERR: [service-id: %d] failed on invoke ReleaseService: %v", x.serviceId, err)
		x.availability.BlackoutWithin(x.timingConfig.ServiceReleaseFailureBlackoutPeriod)
	} else {
		defer cancel()
		log.Printf("INFO: [service-id: %d] waiting for service release", x.serviceId)
		err = <-c
		if nil == err {
			x.availability.BlackoutWithin(x.timingConfig.ServiceReleaseSuccessBlackoutPeriod)
			log.Printf("INFO: [service-id: %d] service released", x.serviceId)
		} else {
			x.availability.BlackoutWithin(x.timingConfig.ServiceReleaseFailureBlackoutPeriod)
			log.Printf("WARN: [service-id: %d] failed on releasing service: %v", x.serviceId, err)
		}
	}
	x.servicing.Store(false)
	return x.runOffServiceSelfCheck, x.durationToNextOffServiceSelfCheck()
}

func (x *ServiceRack) stopService() {
	if true == x.servicing.Load() {
		log.Printf("INFO: [service-id: %d] attempt to stop service", x.serviceId)
		x.stateTransit.AppendDetour(func() {
			x.runServiceRelease()
		})
	}
}

// Answers service activation requests from remote peers.
// Intent to be invoke by service communication adapter
func (x *ServiceRack) RequestServiceActivationApproval(nodeId int32, forceActivation bool) (accept bool) {
	if true == x.serviceActivating.Load() {
		return false
	}
	localAvailability := x.availability.Availability()
	if (false == forceActivation) && (true == localAvailability) {
		if true == x.servicing.Load() {
			return false
		}
		if false == x.isFrontNodeId(nodeId) {
			return false
		}
	}
	log.Printf("INFO: [service-id: %d] about to approve service activation: local-availability=%v, peer-node-id=%d, force=%v, ", x.serviceId, localAvailability, nodeId, forceActivation)
	x.stopService()
	return true
}

func (x *ServiceRack) ServiceTakeOver() (err error) {
	log.Printf("INFO: [service-id: %d] attempt to take over service", x.serviceId)
	ch := make(chan error, 1)
	x.stateTransit.AppendDetour(func() {
		ch <- x.activateService(true)
	})
	err = <-ch
	if nil == err {
		log.Printf("INFO: [service-id: %d] service take over complete", x.serviceId)
	} else {
		log.Printf("INFO: [service-id: %d] service take over failed: %v", x.serviceId, err)
	}
	return err
}

// Answers service status query requests from remote peers.
// Intent to be invoke by service communication adapter
func (x *ServiceRack) IsOnService() (onService bool) {
	runningService := x.servicing.Load()
	localAvailability := x.availability.Availability()
	if runningService && localAvailability {
		return true
	}
	log.Printf("INFO: [service-id: %d] reply off-service for IsOnService query: running-service=%v, local-availabity=%v", x.serviceId, runningService, localAvailability)
	return false
}

func (x *ServiceRack) Close() (err error) {
	x.stopService()
	ch := make(chan error, 1)
	x.stateTransit.AppendDetour(func() {
		ch <- x.runClose()
	})
	err = <-ch
	x.stateTransit.Stop()
	return err
}

func (x *ServiceRack) StateTransitionLoop() (err error) {
	x.startNodeLoops()
	defer x.stopNodeLoops()
	go x.controlRunner.RunLoop()
	defer x.controlRunner.Close()
	if err = x.runPrepare(); nil != err {
		return err
	}
	return x.stateTransit.RunStateTransportLoop(x.runOffServiceSelfCheck)
}
