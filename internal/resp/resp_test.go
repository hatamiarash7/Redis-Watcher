package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestWriteCommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"monitor", []string{"MONITOR"}, "*1\r\n$7\r\nMONITOR\r\n"},
		{"empty argument", []string{"AUTH", ""}, "*2\r\n$4\r\nAUTH\r\n$0\r\n\r\n"},
		{"byte lengths and embedded CRLF", []string{"é\r\n"}, "*1\r\n$4\r\né\r\n\r\n"},
		{"no arguments", nil, "*0\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := WriteCommand(bufio.NewWriter(&out), tt.args...); err != nil {
				t.Fatal(err)
			}
			if got := out.String(); got != tt.want {
				t.Errorf("wire command = %q, want %q", got, tt.want)
			}
		})
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWriteCommandPropagatesWriteErrors(t *testing.T) {
	want := errors.New("write failed")
	tests := []struct {
		name string
		size int
		args []string
	}{
		{"flush", 4096, []string{"MONITOR"}},
		{"argument write", 16, []string{strings.Repeat("x", 32)}},
		{"array header write", 1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bufio.NewWriterSize(failingWriter{want}, tt.size)
			if err := WriteCommand(w, tt.args...); !errors.Is(err, want) {
				t.Errorf("error = %v, want %v", err, want)
			}
		})
	}
}

func TestReadSimpleString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"ok", "+OK\r\n", ""},
		{"other simple string", "+QUEUED\r\n", ""},
		{"redis error", "-ERR denied\r\n", "ERR denied"},
		{"empty response", "\r\n", "empty response"},
		{"wrong frame", ":1\r\n", "unexpected response: :1"},
		{"truncated", "+OK", "EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ReadSimpleString(bufio.NewReader(strings.NewReader(tt.input)))
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tt.want {
				t.Errorf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestReadBulkString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		errText string
		errIs   error
	}{
		{name: "body", input: "$5\r\nhello\r\n", want: "hello"},
		{name: "empty body", input: "$0\r\n\r\n"},
		{name: "binary body", input: "$4\r\na\x00\r\n\r\n", want: "a\x00\r\n"},
		{name: "null", input: "$-1\r\n", errIs: io.EOF},
		{name: "missing header", input: "", errIs: io.EOF},
		{name: "empty header", input: "\r\n", errText: "empty bulk header"},
		{name: "redis error", input: "-ERR denied\r\n", errText: "ERR denied"},
		{name: "wrong frame", input: "+OK\r\n", errText: "expected bulk string"},
		{name: "invalid length", input: "$abc\r\n", errText: "invalid bulk length"},
		{name: "missing body", input: "$4\r\n", errIs: io.EOF},
		{name: "truncated body", input: "$4\r\nab", errIs: io.ErrUnexpectedEOF},
		{name: "truncated terminator", input: "$2\r\nab\r", errIs: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadBulkString(bufio.NewReader(strings.NewReader(tt.input)))
			if got != tt.want {
				t.Errorf("body = %q, want %q", got, tt.want)
			}
			switch {
			case tt.errIs != nil:
				if !errors.Is(err, tt.errIs) {
					t.Errorf("error = %v, want %v", err, tt.errIs)
				}
			case tt.errText != "":
				if err == nil || !strings.Contains(err.Error(), tt.errText) {
					t.Errorf("error = %v, want containing %q", err, tt.errText)
				}
			case err != nil:
				t.Fatal(err)
			}
		})
	}
}

func TestReadersLeaveFollowingFrame(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("+OK\r\n$3\r\nfoo\r\n+OK\r\n"))
	if err := ReadSimpleString(r); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadBulkString(r); err != nil || got != "foo" {
		t.Fatalf("bulk = %q, %v", got, err)
	}
	if err := ReadSimpleString(r); err != nil {
		t.Fatal(err)
	}
}

func TestAuthArgs(t *testing.T) {
	for _, tt := range []struct {
		user string
		want []string
	}{
		{"", []string{"AUTH", "test-password"}},
		{"watcher", []string{"AUTH", "watcher", "test-password"}},
	} {
		if got := AuthArgs(tt.user, "test-password"); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("AuthArgs(%q) = %v, want %v", tt.user, got, tt.want)
		}
	}
}
