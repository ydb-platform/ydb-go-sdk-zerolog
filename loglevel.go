package zerolog

import (
	"github.com/rs/zerolog"
)

func logDebugWarn(logger *zerolog.Logger, err error) *zerolog.Event {
	return logLevel(logger, err, zerolog.DebugLevel, zerolog.WarnLevel)
}

func logDebugInfo(logger *zerolog.Logger, err error) *zerolog.Event {
	return logLevel(logger, err, zerolog.DebugLevel, zerolog.InfoLevel)
}

func logInfoWarn(logger *zerolog.Logger, err error) *zerolog.Event {
	return logLevel(logger, err, zerolog.InfoLevel, zerolog.WarnLevel)
}

func logLevel(logger *zerolog.Logger, err error, okLevel, errLevel zerolog.Level) *zerolog.Event {
	level := okLevel
	if err != nil {
		level = errLevel
	}

	levelLogger := logger.Level(level)
	return levelLogger.Err(err)
}
