// Package resp implements just enough of the Redis serialization protocol
// (RESP2) to support the commands Redis Watcher actually issues over the
// raw connections it owns: AUTH, MONITOR, INFO and ROLE.
//
// This is intentionally minimal — when the daemon needs a real client (for
// SUBSCRIBE, transactions, etc.) it should pull in github.com/redis/go-redis
// instead.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// WriteCommand writes a RESP array command to w and flushes it.
func WriteCommand(w *bufio.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return w.Flush()
}

// ReadSimpleString reads a single reply and returns nil for "+OK", an error
// for "-...", and an error for any other frame.
func ReadSimpleString(r *bufio.Reader) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return errors.New("empty response")
	}
	switch line[0] {
	case '+':
		return nil
	case '-':
		return errors.New(line[1:])
	default:
		return fmt.Errorf("unexpected response: %s", line)
	}
}

// ReadBulkString reads a $<n>\r\n<n bytes>\r\n bulk string and returns its
// body. A null bulk string ($-1) is returned as ("", io.EOF) so callers can
// distinguish it from a successful empty string.
func ReadBulkString(r *bufio.Reader) (string, error) {
	head, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	head = strings.TrimRight(head, "\r\n")
	if len(head) == 0 {
		return "", errors.New("empty bulk header")
	}
	if head[0] == '-' {
		return "", errors.New(head[1:])
	}
	if head[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got %q", head)
	}
	n, err := strconv.Atoi(head[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk length %q: %w", head, err)
	}
	if n < 0 {
		return "", io.EOF
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// AuthArgs builds the argument list for an AUTH command, optionally with a
// username (Redis 6+ ACL form).
func AuthArgs(user, pass string) []string {
	if user != "" {
		return []string{"AUTH", user, pass}
	}
	return []string{"AUTH", pass}
}
