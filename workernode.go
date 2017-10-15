package watchback

type WorkerNode struct {
	nodeId int32
	messenger NodeMessagingAdapter
}

func newWorkerNode(nodeId int32, messenger NodeMessagingAdapter) (workerNode * WorkerNode) {
	return &WorkerNode {
		nodeId: nodeId,
		messenger: messenger,
	}
}
