package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// captureStdout перехватывает os.Stdout для fn
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("не удалось создать os.Pipe: %v", err)
	}

	originalStdout := os.Stdout
	os.Stdout = writer
	defer func() {
		os.Stdout = originalStdout
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("не удалось закрыть writer: %v", err)
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		t.Fatalf("не удалось прочитать из pipe: %v", err)
	}

	return buf.String()
}

func TestParseLevel(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected slog.Level
	}{
		{name: "debug level", input: LevelDebug, expected: slog.LevelDebug},
		{name: "warn level", input: LevelWarn, expected: slog.LevelWarn},
		{name: "error level", input: LevelError, expected: slog.LevelError},
		{name: "info level", input: LevelInfo, expected: slog.LevelInfo},
		{name: "unknown level is set to info", input: "trace", expected: slog.LevelInfo},
		{name: "empty string is set to info", input: "", expected: slog.LevelInfo},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result := parseLevel(testCase.input)
			if result != testCase.expected {
				t.Errorf("parseLevel(%q) = %v, ожидалось %v", testCase.input, result, testCase.expected)
			}
		})
	}
}

func TestInit_JSONHandler(t *testing.T) {
	output := captureStdout(t, func() {
		Init(LevelInfo, HandlerJSON)
		slog.Info("test message", "user_id", 42)
	})

	var logEntry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &logEntry); err != nil {
		t.Fatalf("вывод не является валидным JSON: %v\nвывод: %s", err, output)
	}

	if logEntry[slog.MessageKey] != "test message" {
		t.Errorf("msg = %v, ожидалось 'test message'", logEntry[slog.MessageKey])
	}

	if userID, ok := logEntry["user_id"].(float64); !ok || userID != 42 {
		t.Errorf("user_id = %v, ожидалось 42", logEntry["user_id"])
	}
}

func TestInit_TextHandler(t *testing.T) {
	output := captureStdout(t, func() {
		Init(LevelInfo, HandlerText)
		slog.Info("test message", "user_id", 42)
	})

	if !strings.Contains(output, "msg=\"test message\"") {
		t.Errorf("вывод не содержит ожидаемое msg-поле: %s", output)
	}
	if !strings.Contains(output, "user_id=42") {
		t.Errorf("вывод не содержит ожидаемое user_id-поле: %s", output)
	}
}

func TestInit_TimeFormat(t *testing.T) {
	output := captureStdout(t, func() {
		Init(LevelInfo, HandlerJSON)
		slog.Info("time format check")
	})

	var logEntry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &logEntry); err != nil {
		t.Fatalf("вывод не является валидным JSON: %v", err)
	}

	timeValue, ok := logEntry[slog.TimeKey].(string)
	if !ok {
		t.Fatalf("поле time отсутствует или имеет неверный тип: %v", logEntry[slog.TimeKey])
	}

	timeFormatPattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	if !timeFormatPattern.MatchString(timeValue) {
		t.Errorf("time = %q не соответствует формату '2006-01-02 15:04:05'", timeValue)
	}
}

func TestInit_LevelFiltering(t *testing.T) {
	output := captureStdout(t, func() {
		Init(LevelWarn, HandlerText)
		slog.Debug("должно быть отфильтровано")
		slog.Info("должно быть отфильтровано")
		slog.Warn("должно пройти")
		slog.Error("должно пройти")
	})

	if strings.Contains(output, "должно быть отфильтровано") {
		t.Errorf("сообщения ниже уровня Warn не были отфильтрованы: %s", output)
	}

	warnCount := strings.Count(output, "должно пройти")
	if warnCount != 2 {
		t.Errorf("ожидалось 2 прошедших сообщения (warn+error), получено %d: %s", warnCount, output)
	}
}

func TestInit_UnknownHandlerType_IsSetToText(t *testing.T) {
	output := captureStdout(t, func() {
		Init(LevelWarn, "unknown-handler-type")
		slog.Debug("должно быть отфильтровано")
		slog.Warn("должно пройти", "user_id", 42)
	})

	if strings.Contains(output, "должно быть отфильтровано") {
		t.Errorf("level не применился при unknown handler type: %s", output)
	}

	if !strings.Contains(output, "msg=\"должно пройти\"") {
		t.Errorf("вывод не в текстовом формате или сообщение отсутствует: %s", output)
	}
	if !strings.Contains(output, "user_id=42") {
		t.Errorf("вывод не содержит ожидаемое user_id-поле: %s", output)
	}

	var jsonProbe map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &jsonProbe); err == nil {
		t.Errorf("вывод оказался валидным JSON, ожидался текстовый формат: %s", output)
	}
}

func TestInit_DoesNotPanic(t *testing.T) {
	levels := []string{LevelDebug, LevelInfo, LevelWarn, LevelError, "unknown-level", ""}
	handlerTypes := []string{HandlerJSON, HandlerText, "unknown-type", ""}

	for _, level := range levels {
		for _, handlerType := range handlerTypes {
			t.Run(level+"_"+handlerType, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Init(%q, %q) вызвал панику: %v", level, handlerType, r)
					}
				}()
				captureStdout(t, func() {
					Init(level, handlerType)
				})
			})
		}
	}
}

func TestInitLogic_TimeFormat(t *testing.T) {
	output := captureStdout(t, func() {
		initLogic()
		fixedTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

		slog.Info("time format check", slog.Time("time", fixedTime))
	})

	if !strings.Contains(output, "time=\"2026-01-01 00:00:00\"") {
		t.Errorf("поле time отсутствует или имеет неверный тип: %s", output)
	}
}
