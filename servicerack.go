package watchback

import (
	"fmt"
)

type ServiceRack struct {
	localNodeId int32

	frontNodes []*WorkerNode
	allNodes []*WorkerNode
	frontNodesReady bool
}

func newServiceRack(localNodeId int32) (serviceRack * ServiceRack) {
	return &ServiceRack {
		localNodeId: localNodeId,
		frontNodes: make([]*WorkerNode, 0),
		allNodes: make([]*WorkerNode, 0),
		frontNodesReady: false,
	}
}

func (x * ServiceRack) AddNode(nodeId int32, messenger NodeMessagingAdapter) (workerNode * WorkerNode, err error) {
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
	workerNode = newWorkerNode(nodeId, messenger)
	if !x.frontNodesReady {
		x.frontNodes = append(x.frontNodes, workerNode)
	}
	x.allNodes = append(x.allNodes, workerNode)
	return workerNode, nil
}
