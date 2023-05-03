package zerolog

import (
	"context"
	"strings"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

var _ log.Logger = adapter{}

type adapter struct {
	l *zerolog.Logger
}

func (a adapter) Log(ctx context.Context, msg string, fields ...log.Field) {
	appendFields(
		a.l.WithLevel(level(ctx)).Str("namespace", strings.Join(log.NamesFromContext(ctx), ".")),
		fields,
	).Msg(msg)
}

func level(ctx context.Context) zerolog.Level {
	switch log.LevelFromContext(ctx) {
	case log.TRACE:
		return zerolog.TraceLevel
	case log.DEBUG:
		return zerolog.DebugLevel
	case log.INFO:
		return zerolog.InfoLevel
	case log.WARN:
		return zerolog.WarnLevel
	case log.ERROR:
		return zerolog.ErrorLevel
	case log.FATAL:
		return zerolog.FatalLevel
	default:
		return zerolog.NoLevel
	}
}

func fieldToField(e *zerolog.Event, field log.Field) *zerolog.Event {
	switch field.Type() {
	case log.IntType:
		return e.Int(field.Key(), field.IntValue())
	case log.Int64Type:
		return e.Int64(field.Key(), field.Int64Value())
	case log.StringType:
		return e.Str(field.Key(), field.StringValue())
	case log.BoolType:
		return e.Bool(field.Key(), field.BoolValue())
	case log.DurationType:
		return e.Dur(field.Key(), field.DurationValue())
	case log.StringsType:
		return e.Strs(field.Key(), field.StringsValue())
	case log.ErrorType:
		return e.Err(field.ErrorValue())
	case log.StringerType:
		return e.Stringer(field.Key(), field.Stringer())
	default:
		return e.Interface(field.Key(), field.AnyValue())
	}
}

func appendFields(e *zerolog.Event, fields []log.Field) *zerolog.Event {
	for _, f := range fields {
		e = fieldToField(e, f)
	}
	return e
}
