package monitor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// commandsWithSubcommand is the set of Redis commands that route by their
// first argument. The list mirrors the most common administrative commands
// where the subcommand significantly changes intent.
var commandsWithSubcommand = map[string]struct{}{
	"CLIENT":   {},
	"CONFIG":   {},
	"ACL":      {},
	"SCRIPT":   {},
	"DEBUG":    {},
	"CLUSTER":  {},
	"COMMAND":  {},
	"FUNCTION": {},
	"LATENCY":  {},
	"MEMORY":   {},
	"MODULE":   {},
	"OBJECT":   {},
	"PUBSUB":   {},
	"SLOWLOG":  {},
	"XGROUP":   {},
	"XINFO":    {},
}

// Parse parses a single Redis MONITOR output line.
//
// The format of a MONITOR line is:
//
//	+1583789991.940458 [0 127.0.0.1:54302] "PING"
//	+1583789992.940458 [0 127.0.0.1:54302] "SET" "foo" "bar with \"quotes\""
//	+1583789993.940458 [0 [::1]:54302] "GET" "key"
//
// The leading "+" (the RESP simple-string marker) is tolerated but optional;
// callers reading from a RESP stream typically include it.
func Parse(line string) (*event.Event, error) {
	original := line
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, errors.New("empty line")
	}
	if line[0] == '+' {
		line = line[1:]
	}

	spaceIdx := strings.IndexByte(line, ' ')
	if spaceIdx <= 0 {
		return nil, fmt.Errorf("invalid format: no space after timestamp: %q", original)
	}

	ts, err := parseTimestamp(line[:spaceIdx])
	if err != nil {
		return nil, err
	}
	rest := line[spaceIdx+1:]

	db, src, rest, err := parseBracket(rest)
	if err != nil {
		return nil, err
	}

	args, err := parseQuotedArgs(rest)
	if err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}
	if len(args) == 0 {
		return nil, errors.New("no command in line")
	}

	cmd := strings.ToUpper(args[0])
	ev := &event.Event{
		Timestamp: ts,
		DB:        db,
		Source:    src,
		Command:   cmd,
		Args:      args[1:],
	}
	if _, ok := commandsWithSubcommand[cmd]; ok && len(ev.Args) > 0 {
		ev.Subcommand = strings.ToUpper(ev.Args[0])
	}
	return ev, nil
}

func parseTimestamp(s string) (time.Time, error) {
	// MONITOR prints "<sec>.<frac>" where <frac> is microsecond-precision.
	// We parse the two halves as integers to avoid the floating-point
	// rounding that crept in when we used ParseFloat.
	dot := strings.IndexByte(s, '.')
	secStr, fracStr := s, ""
	if dot != -1 {
		secStr, fracStr = s[:dot], s[dot+1:]
	}
	sec, err := strconv.ParseInt(secStr, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp %q: %w", s, err)
	}
	if sec < 0 {
		return time.Time{}, fmt.Errorf("negative timestamp: %s", s)
	}
	var nsec int64
	if fracStr != "" {
		if len(fracStr) > 9 {
			fracStr = fracStr[:9]
		}
		f, err := strconv.ParseInt(fracStr, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid timestamp fraction %q: %w", fracStr, err)
		}
		// Pad to nanoseconds.
		for i := len(fracStr); i < 9; i++ {
			f *= 10
		}
		nsec = f
	}
	return time.Unix(sec, nsec).UTC(), nil
}

func parseBracket(rest string) (int, event.Source, string, error) {
	if len(rest) == 0 || rest[0] != '[' {
		return 0, event.Source{}, "", fmt.Errorf("missing '[' before db/source: %q", rest)
	}
	// The bracketed segment can itself contain an IPv6 source address like
	// "[0 [::1]:54302]" so a naive IndexByte(']') misses the outer one. Search
	// for the closing bracket that is followed by a space and a quote, which
	// is the canonical boundary between the metadata and the first argument.
	closeIdx := -1
	for i := 1; i < len(rest); i++ {
		if rest[i] != ']' {
			continue
		}
		// Look ahead for whitespace and then a quote (or end of input). This
		// is unambiguous because Redis always emits the command name as a
		// quoted argument.
		j := i + 1
		for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
			j++
		}
		if j == len(rest) || rest[j] == '"' {
			closeIdx = i
			break
		}
	}
	if closeIdx == -1 {
		return 0, event.Source{}, "", errors.New("missing ']' in db/source segment")
	}
	body := rest[1:closeIdx]
	remainder := strings.TrimLeft(rest[closeIdx+1:], " \t")

	sp := strings.IndexByte(body, ' ')
	if sp == -1 {
		return 0, event.Source{}, "", fmt.Errorf("missing space in bracket segment %q", body)
	}
	db, err := strconv.Atoi(body[:sp])
	if err != nil {
		return 0, event.Source{}, "", fmt.Errorf("invalid db %q: %w", body[:sp], err)
	}
	ip, port := splitHostPort(body[sp+1:])
	return db, event.Source{IP: ip, Port: port}, remainder, nil
}

// splitHostPort splits "host:port" handling IPv6 "[::1]:port" and
// degenerate forms (lua scripts, unix peers) where the address may have an
// embedded ':' or none at all.
func splitHostPort(addr string) (string, string) {
	if addr == "" {
		return "", ""
	}
	if addr[0] == '[' {
		end := strings.IndexByte(addr, ']')
		if end != -1 {
			ip := addr[1:end]
			if end+1 < len(addr) && addr[end+1] == ':' {
				return ip, addr[end+2:]
			}
			return ip, ""
		}
	}
	last := strings.LastIndexByte(addr, ':')
	if last == -1 {
		return addr, ""
	}
	return addr[:last], addr[last+1:]
}

// parseQuotedArgs parses a sequence of double-quoted, backslash-escaped
// arguments separated by whitespace, matching the format produced by
// `sdscatrepr()` inside Redis.
func parseQuotedArgs(s string) ([]string, error) {
	args := make([]string, 0, 4)
	var sb strings.Builder
	inQuote := false
	i := 0
	for i < len(s) {
		c := s[i]
		if !inQuote {
			switch c {
			case ' ', '\t':
				i++
			case '"':
				inQuote = true
				sb.Reset()
				i++
			default:
				return nil, fmt.Errorf("unexpected character %q at position %d", c, i)
			}
			continue
		}

		if c == '\\' && i+1 < len(s) {
			i++
			next := s[i]
			switch next {
			case 'n':
				sb.WriteByte('\n')
			case 'r':
				sb.WriteByte('\r')
			case 't':
				sb.WriteByte('\t')
			case 'a':
				sb.WriteByte(0x07)
			case 'b':
				sb.WriteByte(0x08)
			case '"', '\\':
				sb.WriteByte(next)
			case 'x':
				if i+2 < len(s) {
					if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
						sb.WriteByte(byte(v))
						i += 3
						continue
					}
				}
				sb.WriteByte(next)
			default:
				sb.WriteByte(next)
			}
			i++
			continue
		}

		if c == '"' {
			args = append(args, sb.String())
			sb.Reset()
			inQuote = false
			i++
			continue
		}
		sb.WriteByte(c)
		i++
	}
	if inQuote {
		return nil, errors.New("unterminated quoted string")
	}
	return args, nil
}
