package app

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/metrics"
	"github.com/hatamiarash7/redis-watcher/internal/monitor"
)

// TestDispatchSkipsIgnoredCommands exercises the early-filter branch in
// the dispatcher by passing nil for outputs/alerts and asserting on the
// metrics counters. Ignored commands must not increment EventsProcessed
// and must instead bump IgnoredEventsTotal.
func TestDispatchSkipsIgnoredCommands(t *testing.T) {
	reg := metrics.New(nil, true, "test", "test")
	mon := monitor.New(monitor.Options{Network: "tcp", Address: "127.0.0.1:0"}, nil, nil, nil, true)
	src := make(chan *event.Event, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := dispatcher{
		reg:     reg,
		mon:     mon,
		source:  src,
		ignored: map[string]struct{}{"PING": {}, "AUTH": {}},
	}
	done := make(chan struct{})
	go func() {
		d.run(ctx)
		close(done)
	}()

	src <- &event.Event{Command: "PING"}
	src <- &event.Event{Command: "PING"}
	src <- &event.Event{Command: "GET", Args: []string{"k"}}
	src <- &event.Event{Command: "AUTH"}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		processed := testutil.ToFloat64(reg.EventsProcessed)
		ignoredPING := testutil.ToFloat64(reg.IgnoredEventsTotal.WithLabelValues("PING"))
		ignoredAUTH := testutil.ToFloat64(reg.IgnoredEventsTotal.WithLabelValues("AUTH"))
		if processed == 1 && ignoredPING == 2 && ignoredAUTH == 1 {
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected 1 processed, 2 ignored PING, 1 ignored AUTH; got processed=%v pingIgnored=%v authIgnored=%v",
		testutil.ToFloat64(reg.EventsProcessed),
		testutil.ToFloat64(reg.IgnoredEventsTotal.WithLabelValues("PING")),
		testutil.ToFloat64(reg.IgnoredEventsTotal.WithLabelValues("AUTH")),
	)
}
