package watchback

import "context"

type NodeMessagingAdapter interface {
	// Return true if fetched status is in Normal state.
	// Return false for protocol which status can be determine within FetchStatus() function.
	// This function might be invoke in parallel with FetchStatus().
	CheckStatusNormal(ctx context.Context) (isNormal bool, err error)

	// Emit service activation announcement to remote node.
	// Return true if service activation approval request is approved.
	RequestServiceActivationApproval(ctx context.Context) (isApproved bool, err error)

	// Close node connection link
	Close(ctx context.Context) (err error)
}

// Represent a local instance of service.
type ServiceControlAdapter interface {
	// Perform initialize. Service rack status is not ready yet when calling this function.
	// Return value will be logged and treated as self-check result.
	Prepare() (err error)

	// Perform initialize. Service rack status is ready before calling this function.
	// Return value will be logged and treated as self-check result.
	Bootup() (err error)

	// Perform self-check to see if local environment is good for running service.
	// Return nil if passed self-check.
	// Return error if failed on self-check.
	PreAcquireSelfCheck() (err error)

	// Acquire service.
	AcquireService() (err error)

	// Perform self-check to see if running service is good.
	PostAcquireSelfCheck() (err error)

	// Releasing service
	ReleaseService() (err error)

	// Release resources allocated by this service adapter
	Close() (err error)
}
