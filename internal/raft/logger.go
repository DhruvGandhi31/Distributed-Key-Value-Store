package raft

import (
	"log"
	"os"
)

// Logger is the logging interface Node depends on, so callers can supply
// their own structured logger later without changing raft internals.
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// debugRaft gates Debugf output. Flip locally for verbose Raft tracing.
var debugRaft = false

type defaultLogger struct {
	l *log.Logger
}

// NewDefaultLogger returns a Logger backed by the standard library log
// package, writing to stderr with the given prefix.
func NewDefaultLogger(prefix string) Logger {
	return &defaultLogger{l: log.New(os.Stderr, prefix, log.LstdFlags)}
}

func (d *defaultLogger) Debugf(format string, args ...interface{}) {
	if !debugRaft {
		return
	}
	d.l.Printf("DEBUG "+format, args...)
}

func (d *defaultLogger) Infof(format string, args ...interface{}) {
	d.l.Printf("INFO "+format, args...)
}

func (d *defaultLogger) Warnf(format string, args ...interface{}) {
	d.l.Printf("WARN "+format, args...)
}

func (d *defaultLogger) Errorf(format string, args ...interface{}) {
	d.l.Printf("ERROR "+format, args...)
}
