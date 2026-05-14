package alert

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pushgatewayRecorder is a tiny HTTP test server that mimics enough of the
// Pushgateway HTTP API for our purposes. The Prometheus pusher issues
// `PUT /metrics/job/<job>/...` requests with the metric exposition format
// in the body, then expects a 2xx response.
type pushgatewayRecorder struct {
	srv       *httptest.Server
	hits      atomic.Int64
	failNext  atomic.Int64 // when >0, the next N requests return 500
	authCheck atomic.Pointer[func(r *http.Request) bool]

	mu       sync.Mutex
	requests []recordedRequest
}

type recordedRequest struct {
	Method   string
	Path     string
	AuthUser string
	AuthPass string
	HasAuth  bool
}

func newPushgatewayRecorder() *pushgatewayRecorder {
	r := &pushgatewayRecorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.hits.Add(1)
		user, pass, hasAuth := req.BasicAuth()
		r.mu.Lock()
		r.requests = append(r.requests, recordedRequest{
			Method: req.Method, Path: req.URL.Path,
			AuthUser: user, AuthPass: pass, HasAuth: hasAuth,
		})
		r.mu.Unlock()
		if fn := r.authCheck.Load(); fn != nil && !(*fn)(req) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.failNext.Load() > 0 {
			r.failNext.Add(-1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	return r
}

func (r *pushgatewayRecorder) URL() string { return r.srv.URL }
func (r *pushgatewayRecorder) close()      { r.srv.Close() }

func (r *pushgatewayRecorder) hitCount() int64 { return r.hits.Load() }

func (r *pushgatewayRecorder) snapshot() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

func sampleAlert() Alert {
	return Alert{
		Timestamp: time.Unix(1700000000, 0).UTC(),
		Command:   "FLUSHALL",
		Source:    "10.0.0.1:6379",
		DB:        0,
		Reason:    "test",
	}
}

func TestPushgatewaySendsToAllURLs(t *testing.T) {
	pg1 := newPushgatewayRecorder()
	defer pg1.close()
	pg2 := newPushgatewayRecorder()
	defer pg2.close()

	ch := NewPushgatewayChannel(PushgatewayOptions{
		URLs:    []string{pg1.URL(), pg2.URL()},
		Job:     "redis-watcher",
		Timeout: 2 * time.Second,
	})

	if err := ch.Send(context.Background(), sampleAlert()); err != nil {
		t.Fatalf("send: %v", err)
	}

	if pg1.hitCount() == 0 {
		t.Errorf("pg1 not hit")
	}
	if pg2.hitCount() == 0 {
		t.Errorf("pg2 not hit")
	}
}

func TestPushgatewayBasicAuthForwardedToEveryURL(t *testing.T) {
	pg1 := newPushgatewayRecorder()
	defer pg1.close()
	pg2 := newPushgatewayRecorder()
	defer pg2.close()

	ch := NewPushgatewayChannel(PushgatewayOptions{
		URLs:     []string{pg1.URL(), pg2.URL()},
		Job:      "redis-watcher",
		Username: "alice",
		Password: "secret",
		Timeout:  2 * time.Second,
	})

	if err := ch.Send(context.Background(), sampleAlert()); err != nil {
		t.Fatalf("send: %v", err)
	}

	for i, pg := range []*pushgatewayRecorder{pg1, pg2} {
		reqs := pg.snapshot()
		if len(reqs) == 0 {
			t.Errorf("pg%d: no requests", i+1)
			continue
		}
		if !reqs[0].HasAuth || reqs[0].AuthUser != "alice" || reqs[0].AuthPass != "secret" {
			t.Errorf("pg%d: missing/wrong basic auth: %+v", i+1, reqs[0])
		}
	}
}

func TestPushgatewayOmitsAuthWhenUnset(t *testing.T) {
	pg := newPushgatewayRecorder()
	defer pg.close()

	ch := NewPushgatewayChannel(PushgatewayOptions{
		URLs: []string{pg.URL()}, Job: "redis-watcher",
	})

	if err := ch.Send(context.Background(), sampleAlert()); err != nil {
		t.Fatalf("send: %v", err)
	}
	reqs := pg.snapshot()
	if len(reqs) == 0 {
		t.Fatal("no request received")
	}
	if reqs[0].HasAuth {
		t.Errorf("auth header should not be present, got user=%q", reqs[0].AuthUser)
	}
}

func TestPushgatewayPartialFailureReturnsAggregatedError(t *testing.T) {
	good := newPushgatewayRecorder()
	defer good.close()
	bad := newPushgatewayRecorder()
	defer bad.close()
	bad.failNext.Store(100) // always fail

	ch := NewPushgatewayChannel(PushgatewayOptions{
		URLs:    []string{good.URL(), bad.URL()},
		Job:     "redis-watcher",
		Timeout: 2 * time.Second,
	})

	err := ch.Send(context.Background(), sampleAlert())
	if err == nil {
		t.Fatal("expected error from partial failure")
	}
	if !strings.Contains(err.Error(), "1/2") {
		t.Errorf("error should mention 1/2 failed, got: %v", err)
	}
	if !strings.Contains(err.Error(), bad.URL()) {
		t.Errorf("error should include the failed URL, got: %v", err)
	}
	if good.hitCount() == 0 {
		t.Errorf("good URL should still have been hit")
	}
}

func TestPushgatewayErrorWhenNoURLs(t *testing.T) {
	ch := NewPushgatewayChannel(PushgatewayOptions{URLs: nil, Job: "j"})
	if err := ch.Send(context.Background(), sampleAlert()); err == nil {
		t.Fatal("expected error when no URLs configured")
	}
}
