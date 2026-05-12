package alert

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// telegramRoundTripper rewrites requests directed at api.telegram.org to a
// test server URL so we don't actually hit Telegram during tests.
type telegramRoundTripper struct {
	target string
	last   *http.Request
	body   []byte
}

func (r *telegramRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(strings.NewReader(string(b)))
	r.body = b
	r.last = req
	resp, err := http.DefaultTransport.RoundTrip(rewrite(req, r.target))
	return resp, err
}

func rewrite(req *http.Request, target string) *http.Request {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = "http"
	clone.URL.Host = strings.TrimPrefix(target, "http://")
	clone.Host = clone.URL.Host
	return clone
}

func TestTelegramOmitsThreadIDWhenZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ch := NewTelegramChannel(TelegramOptions{BotToken: "x", ChatID: "@grp", Timeout: time.Second})
	rt := &telegramRoundTripper{target: srv.URL}
	ch.client.Transport = rt

	if err := ch.Send(context.Background(), Alert{Command: "FLUSHALL", Source: "1.1.1.1:1", Reason: "test"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(rt.body, &payload); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, present := payload["message_thread_id"]; present {
		t.Errorf("message_thread_id should be omitted when thread_id=0, got body=%s", rt.body)
	}
	if payload["chat_id"] != "@grp" {
		t.Errorf("chat_id: %v", payload["chat_id"])
	}
}

func TestTelegramSendsThreadIDWhenSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ch := NewTelegramChannel(TelegramOptions{
		BotToken: "x", ChatID: "@grp", ThreadID: 42, Timeout: time.Second,
	})
	rt := &telegramRoundTripper{target: srv.URL}
	ch.client.Transport = rt

	if err := ch.Send(context.Background(), Alert{Command: "FLUSHALL", Source: "1.1.1.1:1", Reason: "test"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(rt.body, &payload); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	v, ok := payload["message_thread_id"]
	if !ok {
		t.Fatalf("message_thread_id missing from body: %s", rt.body)
	}
	if got, ok := v.(float64); !ok || int(got) != 42 {
		t.Errorf("thread id wrong: got=%v want=42", v)
	}
}

func TestTelegramSurfacesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"ok":false,"description":"chat not found"}`))
	}))
	defer srv.Close()

	ch := NewTelegramChannel(TelegramOptions{BotToken: "x", ChatID: "missing", Timeout: time.Second})
	ch.client.Transport = &telegramRoundTripper{target: srv.URL}

	err := ch.Send(context.Background(), Alert{Command: "X"})
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Errorf("expected 400 error, got %v", err)
	}
}
