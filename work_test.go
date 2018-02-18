package work

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testWorkHandler struct {
	sync.Mutex
	requests  []*request
	responses []string
}

func (w *testWorkHandler) GetRequest(context.Context) (Request, error) {
	w.Lock()
	defer w.Unlock()
	fmt.Println("GetRequest", len(w.requests))
	// simulate fetching requests from somewhere
	r := rand.Intn(10)
	time.Sleep(time.Duration(r) * time.Millisecond)
	if len(w.requests) == 0 {
		return nil, nil
	}
	// get first request
	x, a := w.requests[0], w.requests[1:]
	w.requests = a
	return x, nil
}

func (w *testWorkHandler) RecordResponse(ctx context.Context, r Response, err error) error {
	if err != nil {
		return nil
	}
	w.Lock()
	defer w.Unlock()

	w.responses = append(w.responses, r.Request().Name())

	return nil
}

type request struct {
	name string
}

func (r *request) Name() string {
	return r.name
}

type response struct {
	request *request
}

func (r *response) Request() Request {
	return r.request
}

func testHandleRequest(ctx context.Context, r Request) (Response, error) {
	fmt.Println("testHandleRequest")
	req := r.(*request)
	return &response{
		request: req,
	}, nil
}

func TestManager(t *testing.T) {

	// fill our test request store
	names := []string{"a", "b", "c", "d", "e", "f"}
	w := testWorkHandler{}
	for _, n := range names {
		w.requests = append(w.requests, &request{name: n})
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := New(3, &w, RequestHandlerFunc(testHandleRequest))
	go func() {
		m.Start(ctx)
	}()

	// wait for work to be done
	fmt.Println("waiting...")
	time.Sleep(time.Second * 10)
	cancel()

	require.Equal(t, len(names), len(w.responses))
	for _, n := range names {
		require.Contains(t, w.responses, n)
	}
}
