package logging

import (
	"log/slog"
	"os"
	"strings"
)

func Level() slog.Level {
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}

// New builds a logger from LOG_LEVEL (default WARN) and LOG_FORMAT (json|text, default json).
func New() *slog.Logger {
	opts := &slog.HandlerOptions{Level: Level()}
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
