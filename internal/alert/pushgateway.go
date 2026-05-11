package alert

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// PushgatewayChannel publishes a counter to a Prometheus Pushgateway every
// time a suspicious command fires. Pushgateway metrics are intended for
// short-lived jobs; in this case we use them as a durable alerting signal
// that can be picked up by Alertmanager.
type PushgatewayChannel struct {
	url    string
	job    string
	labels map[string]string
	client *http.Client
}

// NewPushgatewayChannel builds a PushgatewayChannel.
func NewPushgatewayChannel(url, job string, labels map[string]string, timeout time.Duration) *PushgatewayChannel {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &PushgatewayChannel{
		url:    url,
		job:    job,
		labels: labels,
		client: &http.Client{Timeout: timeout},
	}
}

// Name implements Channel.
func (*PushgatewayChannel) Name() string { return "pushgateway" }

// Close implements Channel.
func (*PushgatewayChannel) Close() error { return nil }

// Send implements Channel.
func (p *PushgatewayChannel) Send(ctx context.Context, a Alert) error {
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

	pusher := push.New(p.url, p.job).
		Gatherer(reg).
		Client(p.client)
	for k, v := range p.labels {
		pusher = pusher.Grouping(k, v)
	}

	if err := pusher.PushContext(ctx); err != nil {
		return fmt.Errorf("pushgateway push: %w", err)
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
