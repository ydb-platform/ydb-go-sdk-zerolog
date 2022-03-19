package zerolog

import (
	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(l *zerolog.Logger, details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents != 0 {
		scope := "ydb.retry"
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			idempotent := info.Idempotent
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Bool("idempotent", idempotent).
				Msg("init")
			start := time.Now()
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Msg("intermediate")
				} else {
					m := retry.Check(info.Error)
					log := l.Warn()
					if m.StatusCode() < 0 {
						log = l.Debug()
					}
					log.Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Bool("retryable", m.MustRetry(idempotent)).
						Bool("deleteSession", m.MustDeleteSession()).
						Int32("code", m.StatusCode()).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.RetryLoopDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Int("attempts", info.Attempts).
							Msg("finish")
					} else {
						m := retry.Check(info.Error)
						log := l.Error()
						if m.StatusCode() < 0 {
							log = l.Warn()
						}
						log.Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Bool("retryable", m.MustRetry(idempotent)).
							Bool("deleteSession", m.MustDeleteSession()).
							Int32("code", m.StatusCode()).
							Err(info.Error).
							Msg("finish")
					}
				}
			}
		}
	}
	return t
}
