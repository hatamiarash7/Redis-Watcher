package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// WebhookChannel POSTs alerts to an arbitrary HTTP endpoint as JSON.
type WebhookChannel struct {
	url     string
	method  string
	headers map[string]string
	client  *http.Client
}

// NewWebhookChannel builds a WebhookChannel.
func NewWebhookChannel(url, method string, headers map[string]string, timeout time.Duration) *WebhookChannel {
	if method == "" {
		method = http.MethodPost
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &WebhookChannel{
		url:     url,
		method:  strings.ToUpper(method),
		headers: headers,
		client:  &http.Client{Timeout: timeout},
	}
}

// Name implements Channel.
func (*WebhookChannel) Name() string { return "webhook" }

// Close implements Channel.
func (*WebhookChannel) Close() error { return nil }

// Payload describes the JSON body sent to webhook endpoints.
type Payload struct {
	Timestamp time.Time `json:"timestamp"`
	Reason    string    `json:"reason"`
	Command   string    `json:"command"`
	Source    string    `json:"source"`
	DB        int       `json:"db"`
	Text      string    `json:"text"`
	Event     any       `json:"event,omitempty"`
}

// Send implements Channel.
func (w *WebhookChannel) Send(ctx context.Context, a Alert) error {
	payload := Payload{
		Timestamp: a.Timestamp,
		Reason:    a.Reason,
		Command:   a.Command,
		Source:    a.Source,
		DB:        a.DB,
		Text:      a.Renderer(),
		Event:     a.Event,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, w.method, w.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if _, ok := w.headers["Content-Type"]; !ok {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range w.headers {
		req.Header.Set(k, v)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("webhook status %d: %s", resp.StatusCode, string(preview))
	}
	return nil
}
