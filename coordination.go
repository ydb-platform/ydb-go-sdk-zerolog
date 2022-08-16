package zerolog

import (
	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Coordination(log *zerolog.Logger, details trace.Details, opts ...option) (t trace.Coordination) {
	return t
}
