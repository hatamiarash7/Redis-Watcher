// Package event defines the canonical audit event emitted by the MONITOR
// consumer and consumed by outputs, metrics and the alerting engine.
package event

import (
	"strings"
	"time"
)

// Source represents the originator of a Redis command observed via MONITOR.
type Source struct {
	IP   string `json:"ip"`
	Port string `json:"port"`
}

// Event is a single parsed MONITOR line.
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	DB        int       `json:"db"`
	Source    Source    `json:"source"`
	// Command is the uppercase command name (e.g. "SET", "HSET", "CLIENT").
	Command string `json:"command"`
	// Subcommand is the uppercase first argument for commands that use one
	// (CLIENT KILL, CONFIG GET, SCRIPT LOAD, ACL SETUSER, ...). Empty otherwise.
	Subcommand string `json:"subcommand,omitempty"`
	// Args are the remaining arguments in their parsed form (already
	// unescaped). Args does NOT include the command name itself; it DOES
	// include the subcommand if any.
	Args []string `json:"args,omitempty"`
}

// FullCommand returns the canonical "COMMAND [SUBCOMMAND]" string used for
// matching alert rules.
func (e *Event) FullCommand() string {
	if e.Subcommand == "" {
		return e.Command
	}
	return e.Command + " " + e.Subcommand
}

// CommandLine reconstructs an approximate textual representation of the
// command and its arguments. Arguments are space-separated and quoted only
// when they contain whitespace.
func (e *Event) CommandLine() string {
	var sb strings.Builder
	sb.WriteString(e.Command)
	for _, a := range e.Args {
		sb.WriteByte(' ')
		if strings.ContainsAny(a, " \t\r\n\"") {
			sb.WriteByte('"')
			for i := 0; i < len(a); i++ {
				c := a[i]
				if c == '"' || c == '\\' {
					sb.WriteByte('\\')
				}
				sb.WriteByte(c)
			}
			sb.WriteByte('"')
		} else {
			sb.WriteString(a)
		}
	}
	return sb.String()
}
