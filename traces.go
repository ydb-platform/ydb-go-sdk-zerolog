package zerolog

import (
	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func WithTraces(l *zerolog.Logger, details trace.Details) ydb.Option {
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(l, details)),
		ydb.WithTraceTable(Table(l, details)),
		ydb.WithTraceScripting(Scripting(l, details)),
		ydb.WithTraceScheme(Scheme(l, details)),
		ydb.WithTraceCoordination(Coordination(l, details)),
		ydb.WithTraceRatelimiter(Ratelimiter(l, details)),
		ydb.WithTraceDiscovery(Discovery(l, details)),
	)
}
