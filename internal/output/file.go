package output

import (
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// FileSink writes audit events to a rotated log file.
type FileSink struct {
	mu     sync.Mutex
	w      *lumberjack.Logger
	format string
}

// FileOptions configures a FileSink.
type FileOptions struct {
	Path       string
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
	Compress   bool
	LocalTime  bool
	Format     string
}

// NewFileSink constructs a FileSink with sane defaults applied.
func NewFileSink(o FileOptions) *FileSink {
	if o.MaxSizeMB <= 0 {
		o.MaxSizeMB = 100
	}
	return &FileSink{
		w: &lumberjack.Logger{
			Filename:   o.Path,
			MaxSize:    o.MaxSizeMB,
			MaxBackups: o.MaxBackups,
			MaxAge:     o.MaxAgeDays,
			Compress:   o.Compress,
			LocalTime:  o.LocalTime,
		},
		format: o.Format,
	}
}

// Name implements Sink.
func (*FileSink) Name() string { return "file" }

// Write implements Sink.
func (s *FileSink) Write(ev *event.Event) error {
	buf, err := encode(ev, s.format)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.w.Write(buf)
	return err
}

// Close implements Sink, flushing the underlying file.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Close()
}
