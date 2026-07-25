package router

// Mode defines how received messages are dispatched to the worker pool.
type Mode int

const (
	// Parallel dispatches each message to a randomly selected worker,
	// maximizing throughput without preserving per-group ordering.
	Parallel Mode = iota
	// PerGroupID dispatches messages to a worker chosen by hashing the
	// message group key, preserving ordering within a group.
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
