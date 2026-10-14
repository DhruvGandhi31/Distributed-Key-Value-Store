package raft

import (
	"log/slog"
	"os"
)

// Logger is the logging interface Node depends on, shaped to match
// *slog.Logger's own method set exactly — so a real *slog.Logger (JSON or
// text, with whatever fields a caller has already attached via .With, e.g.
// nodeID) satisfies this interface directly, with no adapter needed.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NewDefaultLogger returns a Logger backed by log/slog's text handler,
// writing to stderr, with the given prefix attached as a "component"
// field on every line. Callers that want JSON output or other handlers
// construct their own *slog.Logger instead — it already satisfies Logger.
func NewDefaultLogger(prefix string) Logger {
	l := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if prefix != "" {
		l = l.With("component", prefix)
	}
	return l
}
