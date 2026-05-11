package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// TelegramChannel sends alerts via the Telegram Bot API.
type TelegramChannel struct {
	botToken string
	chatID   string
	client   *http.Client
}

// NewTelegramChannel constructs a TelegramChannel.
func NewTelegramChannel(botToken, chatID string, timeout time.Duration) *TelegramChannel {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &TelegramChannel{
		botToken: botToken,
		chatID:   chatID,
		client:   &http.Client{Timeout: timeout},
	}
}

// Name implements Channel.
func (*TelegramChannel) Name() string { return "telegram" }

// Close implements Channel. The HTTP client requires no cleanup.
func (*TelegramChannel) Close() error { return nil }

type telegramRequest struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode,omitempty"`
}

// Send implements Channel.
func (t *TelegramChannel) Send(ctx context.Context, a Alert) error {
	body, err := json.Marshal(telegramRequest{
		ChatID: t.chatID,
		Text:   a.Renderer(),
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)
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
