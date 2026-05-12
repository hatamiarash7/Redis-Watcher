package alert

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// telegramRecorder is a small httptest helper that captures the most recent
// request body and path so tests can assert what the channel sent.
type telegramRecorder struct {
	srv      *httptest.Server
	body     atomic.Pointer[[]byte]
	path     atomic.Pointer[string]
	respCode int
	respBody string
}

func newTelegramRecorder(code int, body string) *telegramRecorder {
	r := &telegramRecorder{respCode: code, respBody: body}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		buf, _ := io.ReadAll(req.Body)
		r.body.Store(&buf)
		p := req.URL.Path
		r.path.Store(&p)
		w.WriteHeader(r.respCode)
		_, _ = w.Write([]byte(r.respBody))
	}))
	return r
}

func (r *telegramRecorder) URL() string { return r.srv.URL }
func (r *telegramRecorder) close()      { r.srv.Close() }

func (r *telegramRecorder) decode(t *testing.T) map[string]any {
	t.Helper()
	bp := r.body.Load()
	if bp == nil {
		t.Fatal("no request received")
	}
	var m map[string]any
	if err := json.Unmarshal(*bp, &m); err != nil {
		t.Fatalf("decode body: %v (%q)", err, string(*bp))
	}
	return m
}

func TestTelegramOmitsThreadIDWhenZero(t *testing.T) {
	rec := newTelegramRecorder(http.StatusOK, `{"ok":true}`)
	defer rec.close()

	ch := NewTelegramChannel(TelegramOptions{
		Endpoint: rec.URL(),
		BotToken: "tok", ChatID: "@grp",
		Timeout: time.Second,
	})

	if err := ch.Send(context.Background(), Alert{Command: "FLUSHALL", Source: "1.1.1.1:1", Reason: "test"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	payload := rec.decode(t)
	if _, present := payload["message_thread_id"]; present {
		t.Errorf("message_thread_id should be omitted when thread_id=0")
	}
}

func TestTelegramSendsThreadIDWhenSet(t *testing.T) {
	rec := newTelegramRecorder(http.StatusOK, `{"ok":true}`)
	defer rec.close()

	ch := NewTelegramChannel(TelegramOptions{
		Endpoint: rec.URL(),
		BotToken: "tok", ChatID: "@grp", ThreadID: 42,
		Timeout: time.Second,
	})

	if err := ch.Send(context.Background(), Alert{Command: "FLUSHALL", Source: "1.1.1.1:1", Reason: "test"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	payload := rec.decode(t)
	v, ok := payload["message_thread_id"]
	if !ok {
		t.Fatalf("message_thread_id missing")
	}
	if got, ok := v.(float64); !ok || int(got) != 42 {
		t.Errorf("thread id wrong: got=%v want=42", v)
	}
}

func TestTelegramSurfacesAPIError(t *testing.T) {
	rec := newTelegramRecorder(http.StatusBadRequest, `{"ok":false,"description":"chat not found"}`)
	defer rec.close()

	ch := NewTelegramChannel(TelegramOptions{
		Endpoint: rec.URL(),
		BotToken: "tok", ChatID: "missing",
		Timeout: time.Second,
	})

	err := ch.Send(context.Background(), Alert{Command: "X"})
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Errorf("expected 400 error, got %v", err)
	}
}

func TestTelegramCustomEndpointAndTokenInPath(t *testing.T) {
	rec := newTelegramRecorder(http.StatusOK, `{"ok":true}`)
	defer rec.close()

	// Trailing slash should be normalized away by the constructor.
	ch := NewTelegramChannel(TelegramOptions{
		Endpoint: rec.URL() + "/",
		BotToken: "1234:abcd", ChatID: "@grp",
		Timeout: time.Second,
	})

	if err := ch.Send(context.Background(), Alert{Command: "X"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	if got := ch.endpoint; got != rec.URL() {
		t.Errorf("endpoint not normalized: got=%q", got)
	}

	pp := rec.path.Load()
	if pp == nil {
		t.Fatal("no request received")
	}
	if want := "/bot1234:abcd/sendMessage"; *pp != want {
		t.Errorf("path: got=%q want=%q", *pp, want)
	}
}

func TestTelegramDefaultEndpoint(t *testing.T) {
	ch := NewTelegramChannel(TelegramOptions{BotToken: "x", ChatID: "y"})
	if ch.endpoint != DefaultTelegramEndpoint {
		t.Errorf("default endpoint: got=%q want=%q", ch.endpoint, DefaultTelegramEndpoint)
	}
}
