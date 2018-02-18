package work

import (
	"context"
	"fmt"
	"time"
)

// WorkHandler gets and reports jobs. Must be safe for concurrent use.
type WorkHandler interface {
	// Get a work request from queue, database, etc
	GetRequest(context.Context) (Request, error)
	// Record a response done - possibly with an error
	RecordResponse(context.Context, Response, error) error
}

// these are simple interfaces. mostly to avoid naively passing around interface{}

// Request is a request to do some work. This is just an example
type Request interface {
	// Name returns a name that identifies the requested work
	Name() string
}

type Response interface {
	// Request returns the request associated with the response
	Request() Request
}

type RequestHandler interface {
	Handle(context.Context, Request) (Response, error)
}

// RequestHandlerFunc wraps a function as a RequestHandler
type RequestHandlerFunc func(context.Context, Request) (Response, error)

func (f RequestHandlerFunc) Handle(ctx context.Context, r Request) (Response, error) {
	return f(ctx, r)
}

// Manager manages getting requests, workers, and recording responses
type Manager struct {
	max            int // maximum in process requests
	workHandler    WorkHandler
	requestHandler RequestHandler
}

// New creates a new manager.
func New(max int, w WorkHandler, r RequestHandler) *Manager {
	return &Manager{
		max:            max,
		workHandler:    w,
		requestHandler: r,
	}
}

// wait for up to waitTime for done signals.
// returns number of done signals received
func waitForDone(done chan struct{}, waitTime time.Duration) int {
	count := 0
	timer := time.NewTimer(waitTime)
DONE:
	for {
		select {
		case <-done:
			count++
		case <-timer.C:
			break DONE
		}
	}
	timer.Stop()

	return count
}

// Start starts the manager.
// This polls for work by calling GetRequest.
// It attempts to avoid polling in a tight loop when there is no work available.
func (m *Manager) Start(ctx context.Context) error {
	running := 0
	// for letting manager know that worker is done
	done := make(chan struct{})

	for {
		// check if should still be running
		select {
		case <-ctx.Done():
			// wait for all workers to return.
			// We want to avoid leaking goroutines.
			// we could call waitForDone here if waitForDone took a maximum number
			// of done's to wait for - this may leak goroutines, however
			for i := 0; i < running; i++ {
				<-done
			}
			return ctx.Err()
		default:
		}

		// should we get new work?
		if running < m.max {
			// we are polling for new work
			req, err := m.workHandler.GetRequest(ctx)
			if err != nil {
				// log error, etc
				fmt.Println(err)
			} else {
				if req != nil {
					running++
					handleRequest(ctx, done, m.workHandler, m.requestHandler, req)
				}
			}
		}

		// wait for up to a second for workers to finish
		// this avoids a busy loop just polling for work
		count := waitForDone(done, time.Second)
		running = running - count
	}
}

// handle a request in a goroutine
func handleRequest(ctx context.Context, done chan struct{}, w WorkHandler, handler RequestHandler, r Request) {
	go func() {
		resp, err := handler.Handle(ctx, r)
		if err != nil {
			// log error, etc
			fmt.Println(err)
		}

		err = w.RecordResponse(ctx, resp, err)
		if err != nil {
			// no retries in this simple example. We could
			// retry with a backoff, etc.
			fmt.Println(err)
		}
		// let the manager know that we are done.
		// this will block until manager receives it.
		done <- struct{}{}
	}()
}
