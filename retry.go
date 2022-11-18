package zerolog

import (
	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(l *zerolog.Logger, details trace.Details, opts ...option) (t trace.Retry) {
	if details&trace.RetryEvents != 0 {
		scope := "ydb.retry"
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			idempotent := info.Idempotent
			if info.NestedCall {
				l.Error().Caller().Timestamp().Msg("nested call")
			}
			l.Debug().Caller().Timestamp().Str("scope", scope).
				Bool("idempotent", idempotent).
				Msg("init")
			start := time.Now()
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Msg("intermediate")
				} else {
					f := l.Warn()
					if !ydb.IsYdbError(info.Error) {
						f = l.Debug()
					}
					m := retry.Check(info.Error)
					f.Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Bool("retryable", m.MustRetry(idempotent)).
						Bool("deleteSession", m.MustDeleteSession()).
						Int64("code", m.StatusCode()).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.RetryLoopDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Int("attempts", info.Attempts).
							Msg("finish")
					} else {
						f := l.Error()
						if !ydb.IsYdbError(info.Error) {
							f = l.Debug()
						}
						m := retry.Check(info.Error)
						f.Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Bool("retryable", m.MustRetry(idempotent)).
							Bool("deleteSession", m.MustDeleteSession()).
							Int64("code", m.StatusCode()).
							Err(info.Error).
							Msg("finish")
					}
				}
			}
		}
	}
	return t
}
