package fake

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	"github.com/silviolleite/loafer-awsx/consumer"
)

// Compile-time assertion that SchedulerClient satisfies the
// consumer.SchedulerClient interface.
var _ consumer.SchedulerClient = (*SchedulerClient)(nil)

// SchedulerClient is a configurable test double for the
// consumer.SchedulerClient interface. Each method delegates to a corresponding
// function field, letting tests program the response (or error) for every call.
// When a function field is nil the method returns a nil output and a nil error.
// Every call is recorded so tests can assert on the parameters the code under
// test sent.
//
// SchedulerClient is safe for concurrent use; the recorded call slice is guarded
// by an internal mutex.
type SchedulerClient struct {
	CreateScheduleFunc  func(ctx context.Context, params *scheduler.CreateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
	createScheduleCalls []*scheduler.CreateScheduleInput
	mu                  sync.Mutex
}

// CreateSchedule records the call and delegates to CreateScheduleFunc, or
// returns a nil output and nil error when the function is not set.
func (c *SchedulerClient) CreateSchedule(ctx context.Context, params *scheduler.CreateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	c.mu.Lock()
	c.createScheduleCalls = append(c.createScheduleCalls, params)
	c.mu.Unlock()

	if c.CreateScheduleFunc != nil {
		return c.CreateScheduleFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// CreateScheduleCalls returns a copy of the inputs passed to CreateSchedule, in
// call order.
func (c *SchedulerClient) CreateScheduleCalls() []*scheduler.CreateScheduleInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*scheduler.CreateScheduleInput, len(c.createScheduleCalls))
	copy(out, c.createScheduleCalls)
	return out
}
