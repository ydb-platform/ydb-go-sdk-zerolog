package zerolog

import (
	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func WithTraces(l *zerolog.Logger, details trace.Details, opts ...option) ydb.Option {
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(l, details, opts...)),
		ydb.WithTraceTable(Table(l, details, opts...)),
		ydb.WithTraceScripting(Scripting(l, details, opts...)),
		ydb.WithTraceScheme(Scheme(l, details, opts...)),
		ydb.WithTraceCoordination(Coordination(l, details, opts...)),
		ydb.WithTraceRatelimiter(Ratelimiter(l, details, opts...)),
		ydb.WithTraceDiscovery(Discovery(l, details, opts...)),
		ydb.WithTraceDatabaseSQL(DatabaseSQL(l, details, opts...)),
	)
}
