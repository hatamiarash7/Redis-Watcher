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

// DefaultTelegramEndpoint is the official Telegram Bot API base URL. It is
// used when TelegramOptions.Endpoint is empty.
const DefaultTelegramEndpoint = "https://api.telegram.org"

// TelegramChannel sends alerts via the Telegram Bot API.
type TelegramChannel struct {
	endpoint string
	botToken string
	chatID   string
	threadID int
	client   *http.Client
}

// TelegramOptions configures a TelegramChannel.
type TelegramOptions struct {
	// Endpoint is the base URL of the Bot API. Leave empty to use the
	// official "https://api.telegram.org". Useful values:
	//
	//   - Self-hosted Bot API server: https://bot-api.internal:8081
	//   - Reverse-proxied access: https://tg-proxy.example.com
	//
	// A trailing slash is allowed and normalized away.
	Endpoint string
	BotToken string
	ChatID   string
	// ThreadID, when non-zero, is sent as `message_thread_id` so messages
	// land in a specific topic of a Telegram forum chat. A value of 0
	// (the zero value) means "general thread / not a forum".
	ThreadID int
	Timeout  time.Duration
}

// NewTelegramChannel constructs a TelegramChannel.
func NewTelegramChannel(opts TelegramOptions) *TelegramChannel {
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	endpoint := strings.TrimRight(opts.Endpoint, "/")
	if endpoint == "" {
		endpoint = DefaultTelegramEndpoint
	}
	return &TelegramChannel{
		endpoint: endpoint,
		botToken: opts.BotToken,
		chatID:   opts.ChatID,
		threadID: opts.ThreadID,
		client:   &http.Client{Timeout: opts.Timeout},
	}
}

// Name implements Channel.
func (*TelegramChannel) Name() string { return "telegram" }

// Close implements Channel. The HTTP client requires no cleanup.
func (*TelegramChannel) Close() error { return nil }

type telegramRequest struct {
	ChatID          string `json:"chat_id"`
	Text            string `json:"text"`
	ParseMode       string `json:"parse_mode,omitempty"`
	MessageThreadID int    `json:"message_thread_id,omitempty"`
}

// Send implements Channel.
func (t *TelegramChannel) Send(ctx context.Context, a Alert) error {
	body, err := json.Marshal(telegramRequest{
		ChatID:          t.chatID,
		Text:            a.Renderer(),
		MessageThreadID: t.threadID,
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/bot%s/sendMessage", t.endpoint, t.botToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("telegram status %d: %s", resp.StatusCode, string(payload))
	}
	return nil
}
