package monitor

import (
	"testing"
	"time"
)

func TestParseSimpleCommand(t *testing.T) {
	ev, err := Parse(`+1583789991.940458 [0 127.0.0.1:54302] "PING"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Command != "PING" {
		t.Errorf("command: %q", ev.Command)
	}
	if ev.DB != 0 {
		t.Errorf("db: %d", ev.DB)
	}
	if ev.Source.IP != "127.0.0.1" || ev.Source.Port != "54302" {
		t.Errorf("source: %+v", ev.Source)
	}
	want := time.Unix(1583789991, 940458000).UTC()
	if !ev.Timestamp.Equal(want) {
		t.Errorf("ts got=%v want=%v", ev.Timestamp, want)
	}
}

func TestParseCommandWithArgs(t *testing.T) {
	ev, err := Parse(`+1583789992.123 [3 127.0.0.1:54302] "SET" "user:1" "alice"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Command != "SET" {
		t.Errorf("command: %q", ev.Command)
	}
	if ev.DB != 3 {
		t.Errorf("db: %d", ev.DB)
	}
	if len(ev.Args) != 2 || ev.Args[0] != "user:1" || ev.Args[1] != "alice" {
		t.Errorf("args: %v", ev.Args)
	}
}

func TestParseHandlesEscapedQuotes(t *testing.T) {
	ev, err := Parse(`+1583789991.940458 [0 127.0.0.1:54302] "SET" "k" "a\"b\\c\nd"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Args[1] != "a\"b\\c\nd" {
		t.Errorf("escaped value: %q", ev.Args[1])
	}
}

func TestParseHexEscape(t *testing.T) {
	ev, err := Parse(`+1.0 [0 127.0.0.1:1] "SET" "k" "\x00\x41"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Args[1] != "\x00A" {
		t.Errorf("hex escape: %q", ev.Args[1])
	}
}

func TestParseIPv6Source(t *testing.T) {
	ev, err := Parse(`+1.0 [0 [::1]:54302] "GET" "k"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Source.IP != "::1" || ev.Source.Port != "54302" {
		t.Errorf("ipv6 src: %+v", ev.Source)
	}
}

func TestParseSubcommand(t *testing.T) {
	ev, err := Parse(`+1.0 [0 127.0.0.1:1] "CONFIG" "GET" "maxmemory"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Command != "CONFIG" || ev.Subcommand != "GET" {
		t.Errorf("cmd/sub: %q %q", ev.Command, ev.Subcommand)
	}
	if ev.FullCommand() != "CONFIG GET" {
		t.Errorf("full: %q", ev.FullCommand())
	}
}

func TestParseStripsLeadingPlusAndCRLF(t *testing.T) {
	ev, err := Parse("+1.0 [0 127.0.0.1:1] \"PING\"\r\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Command != "PING" {
		t.Errorf("command: %q", ev.Command)
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]string{
		"empty":                "",
		"no space":             "+1583789991.940458",
		"bad ts":               "+notanumber [0 127.0.0.1:1] \"PING\"",
		"no bracket":           "+1.0 0 127.0.0.1:1 \"PING\"",
		"unterminated bracket": "+1.0 [0 127.0.0.1:1 \"PING\"",
		"bad db":               `+1.0 [x 127.0.0.1:1] "PING"`,
		"unterminated quote":   `+1.0 [0 127.0.0.1:1] "PING`,
		"unexpected outside":   `+1.0 [0 127.0.0.1:1] PING`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(line); err == nil {
				t.Errorf("expected error for %q", line)
			}
		})
	}
}

func TestCommandLineReconstructsCommand(t *testing.T) {
	ev, err := Parse(`+1.0 [0 127.0.0.1:1] "SET" "key with space" "value"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got := ev.CommandLine()
	want := `SET "key with space" value`
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
}

func TestSplitHostPortIPv4(t *testing.T) {
	ip, port := splitHostPort("127.0.0.1:1234")
	if ip != "127.0.0.1" || port != "1234" {
		t.Errorf("got %q %q", ip, port)
	}
}

func TestSplitHostPortIPv6(t *testing.T) {
	ip, port := splitHostPort("[::1]:1234")
	if ip != "::1" || port != "1234" {
		t.Errorf("got %q %q", ip, port)
	}
}
