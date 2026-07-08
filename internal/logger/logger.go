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

// NewLogger конструктор
func NewLogger(typeHandler typeHandler) *slog.Logger {
	var level slog.LevelVar
	level.Set(slog.LevelDebug)

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

	switch typeHandler {
	case TypeHandlerJSON:
		return slog.New(jsonHandler)
	case TypeHandlerText:
		return slog.New(textHandler)
	default:
		return slog.Default()

	}
}
