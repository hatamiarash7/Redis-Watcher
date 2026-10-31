package event

import "testing"

func TestFullCommand(t *testing.T) {
	for _, tt := range []struct {
		event Event
		want  string
	}{
		{Event{Command: "GET"}, "GET"},
		{Event{Command: "CONFIG", Subcommand: "SET"}, "CONFIG SET"},
	} {
		if got := tt.event.FullCommand(); got != tt.want {
			t.Errorf("FullCommand() = %q, want %q", got, tt.want)
		}
	}
}

func TestCommandLine(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
		want string
	}{
		{"no args", Event{Command: "PING"}, "PING"},
		{"plain args", Event{Command: "SET", Args: []string{"key", "value"}}, "SET key value"},
		{"subcommand not duplicated", Event{Command: "CONFIG", Subcommand: "SET", Args: []string{"SET", "key", "value"}}, "CONFIG SET key value"},
		{"space", Event{Command: "SET", Args: []string{"key", "hello world"}}, `SET key "hello world"`},
		{"quote and backslash", Event{Command: "SET", Args: []string{"key", "say \"hi\" \\ end"}}, `SET key "say \"hi\" \\ end"`},
		{"whitespace", Event{Command: "SET", Args: []string{"\t\r\n"}}, "SET \"\t\r\n\""},
		{"unicode", Event{Command: "SET", Args: []string{"key", "سلام دنیا"}}, `SET key "سلام دنیا"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ev.CommandLine(); got != tt.want {
				t.Errorf("CommandLine() = %q, want %q", got, tt.want)
			}
		})
	}
}
