package router

// Mode defines how received messages are dispatched to the worker pool.
type Mode int

const (
	// Parallel hands each message to whichever worker is free next,
	// maximizing throughput without preserving per-group ordering.
	Parallel Mode = iota
	// PerGroupID runs at most one message per group key (MessageGroupId plus
	// any custom group fields) at a time, in receive order, and lets any free
	// worker take any group that has pending messages, preserving ordering
	// within a group without binding a group to a worker.
	PerGroupID
)

// String returns the human-readable name of the Mode. Unknown values are
// rendered together with their numeric value.
func (m Mode) String() string {
	switch m {
	case Parallel:
		return "Parallel"
	case PerGroupID:
		return "PerGroupID"
	default:
		return "Unknown"
	}
}

// valid reports whether the Mode is one of the defined dispatch strategies.
func (m Mode) valid() bool {
	switch m {
	case Parallel, PerGroupID:
		return true
	default:
		return false
	}
}
