package alert

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// PushgatewayChannel publishes a counter to one or more Prometheus
// Pushgateway endpoints every time a suspicious command fires. Pushgateway
// metrics are intended for short-lived jobs; in this case we use them as a
// durable alerting signal that can be picked up by Alertmanager.
//
// Multiple URLs are supported so that operators can run redundant
// Pushgateway instances (e.g. one per AZ) without losing alerts when a
// single gateway is unreachable. The same payload is pushed to every URL
// in parallel; if at least one push fails, Send returns an aggregated
// error.
type PushgatewayChannel struct {
	urls     []string
	job      string
	username string
	password string
	labels   map[string]string
	client   *http.Client
}

// PushgatewayOptions configures a PushgatewayChannel.
type PushgatewayOptions struct {
	// URLs is the list of Pushgateway base URLs to push to. At least one
	// entry is required.
	URLs []string
	// Job is the value of the `job` grouping key Prometheus uses to index
	// the pushed metrics.
	Job string
	// Username and Password, when set, are applied as HTTP Basic Auth
	// credentials to every push request. The same pair is used for every
	// URL — set per-URL credentials by running multiple Channels if you
	// need that.
	Username string
	Password string
	// Labels are additional grouping labels applied to every push.
	Labels map[string]string
	// Timeout is the per-request HTTP timeout.
	Timeout time.Duration
}

// NewPushgatewayChannel builds a PushgatewayChannel.
func NewPushgatewayChannel(opts PushgatewayOptions) *PushgatewayChannel {
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	return &PushgatewayChannel{
		urls:     append([]string{}, opts.URLs...),
		job:      opts.Job,
		username: opts.Username,
		password: opts.Password,
		labels:   opts.Labels,
		client:   &http.Client{Timeout: opts.Timeout},
	}
}

// Name implements Channel.
func (*PushgatewayChannel) Name() string { return "pushgateway" }

// Close implements Channel.
func (*PushgatewayChannel) Close() error { return nil }

// Send implements Channel. It pushes the alert counter to every configured
// URL in parallel. The call succeeds when every push succeeds; if any URL
// fails, Send returns an aggregated error describing each failure.
func (p *PushgatewayChannel) Send(ctx context.Context, a Alert) error {
	if len(p.urls) == 0 {
		return errors.New("pushgateway: no URLs configured")
	}

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Subsystem: "alert",
		Name:      "suspicious_commands_total",
		Help:      "Suspicious commands pushed to the Pushgateway by Redis Watcher.",
	}, []string{"command", "source_ip", "reason"})
	counter.WithLabelValues(a.Command, sourceIP(a.Source), a.Reason).Inc()

	reg := prometheus.NewRegistry()
	if err := reg.Register(counter); err != nil {
		return err
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, url := range p.urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			pusher := push.New(url, p.job).
				Gatherer(reg).
				Client(p.client)
			if p.username != "" || p.password != "" {
				pusher = pusher.BasicAuth(p.username, p.password)
			}
			for k, v := range p.labels {
				pusher = pusher.Grouping(k, v)
			}
			if err := pusher.PushContext(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", url, err))
				mu.Unlock()
			}
		}(url)
	}
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("pushgateway: %d/%d targets failed: %w",
			len(errs), len(p.urls), errors.Join(errs...))
	}
	return nil
}

func sourceIP(source string) string {
	for i := len(source) - 1; i >= 0; i-- {
		if source[i] == ':' {
			return source[:i]
		}
	}
	return source
}
