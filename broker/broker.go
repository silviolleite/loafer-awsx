package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/silviolleite/loafer-awsx/consumer"
	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

// Broker is the top-level orchestrator. It owns a single SQS client and a set
// of routes and, on Run, starts one Consumer per route concurrently. It
// forwards its logger, retry timeout, and global middleware to every consumer,
// coordinates a clean shutdown when the context is canceled, and fails fast if
// any consumer cannot start. A Broker is created with New and run with Run.
type Broker struct {
	sqsClient         consumer.SQSClient
	log               *slog.Logger
	routes            []*router.Route
	globalMiddlewares []middleware.Middleware
	retryTimeout      time.Duration
	shutdownTimeout   time.Duration
}

// New builds a Broker for the given SQS client and routes. The broker defaults
// to a stdout logger, a 5 second retry timeout, and an unbounded shutdown wait
// (it waits until consumers finish); all are overridable through options. Nil
// options are ignored.
//
// New returns errors.ErrNoSQSClient when sqsClient is nil and
// errors.ErrNoRoute when routes is nil or empty. Option failures are wrapped
// with errors.ErrInvalidOption.
func New(sqsClient consumer.SQSClient, routes []*router.Route, opts ...Option) (*Broker, error) {
	if sqsClient == nil {
		return nil, errors.ErrNoSQSClient
	}
	if len(routes) == 0 {
		return nil, errors.ErrNoRoute
	}

	b := &Broker{
		sqsClient:    sqsClient,
		routes:       routes,
		log:          logger.New(),
		retryTimeout: defaultRetryTimeout,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(b); err != nil {
			return nil, errors.Wrap(errors.ErrInvalidOption, err)
		}
	}

	return b, nil
}

// Run starts one consumer per route concurrently and blocks until every
// consumer has stopped.
//
// It derives a child context so it can stop all consumers together. Each
// consumer is built with the broker logger, retry timeout, and global
// middleware (applied outermost). Run fails fast: if any consumer returns a
// non-nil error — typically a queue-resolve failure wrapping
// errors.ErrQueueResolve — the broker records the first such error, logs it at
// Error level, and cancels the child context to stop the remaining consumers.
// After all consumers finish, that first error is returned.
//
// On external context cancellation Run signals every consumer to stop and waits
// for them to drain in-flight work, returning nil for a clean shutdown. By
// default the wait is unbounded so a slow-but-healthy consumer is never
// abandoned; configure WithShutdownTimeout to bound it when a stuck consumer
// must not block Run indefinitely.
func (b *Broker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	for _, route := range b.routes {
		c, err := consumer.New(b.sqsClient, route,
			consumer.WithLogger(b.log),
			consumer.WithRetryTimeout(b.retryTimeout),
			consumer.WithGlobalMiddleware(b.globalMiddlewares...),
		)
		if err != nil {
			setErr(err)
			b.log.Error("failed to create consumer",
				slog.Any("error", err),
			)
			cancel()
			break
		}

		wg.Add(1)
		go func(c *consumer.Consumer, queueName string) {
			defer wg.Done()

			if err := c.Run(ctx); err != nil {
				setErr(err)
				b.log.Error("consumer stopped with error",
					slog.String("queue_name", queueName),
					slog.Any("error", err),
				)
				cancel()
			}
		}(c, route.QueueName())
	}

	b.waitForConsumers(&wg)

	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

// waitForConsumers blocks until every consumer goroutine has finished. When a
// positive shutdown timeout is configured the wait is bounded by it and
// exceeding it is logged at Error level; because consumers stop promptly on
// context cancellation, that timeout path is a safety net rather than the
// normal case. When no timeout is configured (the default) the wait is
// unbounded, so in-flight work always drains and no timer is armed.
func (b *Broker) waitForConsumers(wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	if b.shutdownTimeout <= 0 {
		<-done
		return
	}

	timer := time.NewTimer(b.shutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		b.log.Error("shutdown timeout exceeded waiting for consumers to stop",
			slog.Duration("shutdown_timeout", b.shutdownTimeout),
		)
	}
}
