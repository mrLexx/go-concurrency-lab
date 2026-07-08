package logger

import (
	"log/slog"
	"os"
)

type typeHandler string

const (
	// TypeHandlerJSON тип логера - json
	TypeHandlerJSON typeHandler = "json"
	// TypeHandlerText тип логера - text
	TypeHandlerText typeHandler = "text"
)

const (
	// LevelDebug уровень логирования: Debug
	LevelDebug slog.Level = -4
	// LevelInfo уровень логирования: Info
	LevelInfo slog.Level = 0
	// LevelWarn уровень логирования: Warn
	LevelWarn slog.Level = 4
	// LevelError уровень логирования: Error
	LevelError slog.Level = 8
)

// NewLogger конструктор
func NewLogger(typeHandler typeHandler, l slog.Level) *slog.Logger {
	var level slog.LevelVar
	level.Set(l)

	replaceAttr := func(groups []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.TimeKey {
			t := attr.Value.Time()
			attr.Value = slog.StringValue(t.Format("2006-01-02 15:04:05"))
		}
		return attr
	}

	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level:       &level,
		ReplaceAttr: replaceAttr,
	})

	textHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:       &level,
		ReplaceAttr: replaceAttr,
	})

	var logger *slog.Logger

	switch typeHandler {
	case TypeHandlerJSON:
		logger = slog.New(jsonHandler)
	case TypeHandlerText:
		logger = slog.New(textHandler)
	default:
		logger = slog.Default()

	}
	slog.SetDefault(logger)
	return logger
}
