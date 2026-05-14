package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// QueueDepthCollector is a Prometheus collector that reports the
// instantaneous depth and capacity of each registered channel. Channels
// are registered by name with callbacks that read len() and cap() — the
// callbacks are invoked on every scrape, which is safe and lock-free.
//
// We use this rather than mirroring channel depth into a gauge on every
// send/receive because the producers are on the hot path and a gauge
// update is more expensive than a single len() call at scrape time.
type QueueDepthCollector struct {
	mu sync.RWMutex

	queues []queueRef

	depthDesc *prometheus.Desc
	capDesc   *prometheus.Desc
}

type queueRef struct {
	name     string
	depthFn  func() int
	capacity int
}

// NewQueueDepthCollector builds an empty collector. Register queues with
// Add before passing it to Registry.RegisterCollector.
func NewQueueDepthCollector() *QueueDepthCollector {
	return &QueueDepthCollector{
		depthDesc: prometheus.NewDesc(
			"redis_watcher_queue_depth",
			"Instantaneous depth of an internal channel. Compare against `redis_watcher_queue_capacity` to detect back-pressure.",
			[]string{"queue"}, nil,
		),
		capDesc: prometheus.NewDesc(
			"redis_watcher_queue_capacity",
			"Capacity of an internal channel.",
			[]string{"queue"}, nil,
		),
	}
}

// Add registers a queue. name is the value of the `queue` label; depthFn
// must be cheap and concurrent-safe (a closure over `len(ch)` is the
// expected shape). capacity is captured once at registration because the
// channels we expose never resize.
func (q *QueueDepthCollector) Add(name string, depthFn func() int, capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queues = append(q.queues, queueRef{name: name, depthFn: depthFn, capacity: capacity})
}

// Describe implements prometheus.Collector.
func (q *QueueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- q.depthDesc
	ch <- q.capDesc
}

// Collect implements prometheus.Collector.
func (q *QueueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, r := range q.queues {
		depth := float64(r.depthFn())
		ch <- prometheus.MustNewConstMetric(q.depthDesc, prometheus.GaugeValue, depth, r.name)
		ch <- prometheus.MustNewConstMetric(q.capDesc, prometheus.GaugeValue, float64(r.capacity), r.name)
	}
}
