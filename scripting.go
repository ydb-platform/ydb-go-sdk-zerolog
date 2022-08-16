package zerolog

import (
	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scripting(log *zerolog.Logger, details trace.Details, opts ...option) (t trace.Scripting) {
	return t
}
