package zerolog

import (
	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(log *zerolog.Logger, details trace.Details) (t trace.Retry) {
	return t
}
