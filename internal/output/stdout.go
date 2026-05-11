package output

import (
	"io"
	"os"
	"sync"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// StdoutSink writes audit events to os.Stdout.
type StdoutSink struct {
	mu     sync.Mutex
	w      io.Writer
	format string
}

// NewStdoutSink builds a StdoutSink.
func NewStdoutSink(format string) *StdoutSink {
	return &StdoutSink{w: os.Stdout, format: format}
}

// Name implements Sink.
func (*StdoutSink) Name() string { return "stdout" }

// Write implements Sink.
func (s *StdoutSink) Write(ev *event.Event) error {
	buf, err := encode(ev, s.format)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.w.Write(buf)
	return err
}

// Close implements Sink. Stdout is intentionally not closed.
func (*StdoutSink) Close() error { return nil }
