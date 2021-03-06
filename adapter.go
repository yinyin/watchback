package watchback

import "context"

// NodeMessagingAdapter represents communication interface of remote nodes
type NodeMessagingAdapter interface {
	// Notice adapter there is an error on messaging operation
	HasMessagingFailure(err error)

	// Check if remote node is running service.
	IsOnService(ctx context.Context) (onService bool, err error)

	// Emit service activation announcement to remote node.
	// Return true if service activation approval request is approved.
	RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error)

	// Close node connection link
	Close(ctx context.Context) (err error)
}

// ServiceControlAdapter represents a local instance of service.
type ServiceControlAdapter interface {
	// Perform initialize. Service rack status is not ready yet when calling this function.
	// Return value will be logged and treated as self-check result.
	Prepare(ctx context.Context) (err error)

	// Perform self-check to see if local environment and running service is normal.
	// Return nil if passed self-check.
	// Return error if failed on self-check.
	OnServiceSelfCheck(ctx context.Context) (err error)

	// Perform self-check to see if local environment is good for starting service.
	// Return nil if passed self-check.
	// Return error if failed on self-check.
	OffServiceSelfCheck(ctx context.Context) (err error)

	// Activate service.
	ActivateService(ctx context.Context) (err error)

	// Releasing service
	ReleaseService(ctx context.Context) (err error)

	// Release resources allocated by this service adapter
	Close(ctx context.Context) (err error)
}
