package logger

import (
	"log/slog"
	"os"
)

const (
	// HandlerJSON тип логера - json
	HandlerJSON string = "json"
	// HandlerText тип логера - text
	HandlerText string = "text"
)

const (
	// LevelInfo уровень логирования - информационный
	LevelInfo string = "info"
	// LevelDebug уровень логирования - отладочный
	LevelDebug string = "debug"
	// LevelWarn уровень логирования - предупредительный
	LevelWarn string = "warn"
	// LevelError уровень логирования - ошибочный
	LevelError string = "error"
)

func init() {
	initLogic()
}

func initLogic() {
	replaceAttrFunc := func(groups []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.TimeKey {
			t := attr.Value.Time()
			attr.Value = slog.StringValue(t.Format("2006-01-02 15:04:05"))
		}
		return attr
	}

	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:       parseLevel(LevelInfo),
		ReplaceAttr: replaceAttrFunc,
	})
	slog.SetDefault(slog.New(textHandler))
}

// Init конструктор
func Init(
	level string,
	typeHandler string,
) {
	replaceAttrFunc := func(groups []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.TimeKey {
			t := attr.Value.Time()
			attr.Value = slog.StringValue(t.Format("2006-01-02 15:04:05"))
		}
		return attr
	}
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:       parseLevel(level),
		ReplaceAttr: replaceAttrFunc,
	})

	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:       parseLevel(level),
		ReplaceAttr: replaceAttrFunc,
	})

	var logger *slog.Logger

	switch typeHandler {
	case HandlerJSON:
		logger = slog.New(jsonHandler)
	case HandlerText:
		logger = slog.New(textHandler)
	default:
		logger = slog.New(textHandler)

	}
	slog.SetDefault(logger)
}

func parseLevel(s string) slog.Level {
	switch s {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
