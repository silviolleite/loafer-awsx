package producer

import "github.com/silviolleite/loafer-awsx/idgen"

// Option configures a Producer during construction. Options are applied in
// order by New. Nil options are ignored.
type Option func(*Producer)

// WithGroupIDGenerator configures the Producer to auto-generate the
// MessageGroupId from a message's attributes when the caller does not provide a
// GroupID explicitly. The generator is invoked with the entry's Attributes map;
// its result is used as the GroupID. When gen is nil the option is a no-op.
//
// Auto-generation only applies to FIFO topics, detected by a TopicARN ending in
// ".fifo". For standard topics the generator is never invoked, because SNS
// rejects MessageGroupId on non-FIFO topics. The generator is also never
// invoked for an entry that already carries a non-empty GroupID.
func WithGroupIDGenerator(gen idgen.GroupIDGenerator) Option {
	return func(p *Producer) {
		if gen != nil {
			p.groupIDGen = gen
		}
	}
}

// WithDeduplicationIDGenerator configures the Producer to auto-generate the
// MessageDeduplicationId from a message's attributes when the caller does not
// provide a DeduplicationID explicitly. The generator is invoked with the
// entry's Attributes map; its result is used as the DeduplicationID. When gen
// is nil the option is a no-op.
//
// Auto-generation only applies to FIFO topics, detected by a TopicARN ending in
// ".fifo". For standard topics the generator is never invoked, because SNS
// rejects MessageDeduplicationId on non-FIFO topics. The generator is also
// never invoked for an entry that already carries a non-empty DeduplicationID.
func WithDeduplicationIDGenerator(gen idgen.DeduplicationIDGenerator) Option {
	return func(p *Producer) {
		if gen != nil {
			p.dedupIDGen = gen
		}
	}
}
