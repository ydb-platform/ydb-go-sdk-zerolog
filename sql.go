package zerolog

import (
	"github.com/rs/zerolog"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l *zerolog.Logger, details trace.Details, opts ...option) (t trace.DatabaseSQL) {
	if details&trace.DatabaseSQLEvents == 0 {
		return
	}
	options := parseOptions(opts...)
	scope := "ydb.database.sql"
	if details&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		scope := scope + `.connector`
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("connect start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					l.Info().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`connected`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`connect failed`)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		scope := scope + `.conn`
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("ping start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`ping done`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`ping failed`)
				}
			}
		}
		t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				if info.Error == nil {
					l.Info().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`closed`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`close failed`)
				}
			}
		}
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("begin transaction start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`begin transaction was success`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`begin transaction failed`)
				}
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			if options.logQuery {
				l.Trace().Caller().Timestamp().Str("scope", scope).
					Str("query", info.Query).
					Msg("prepare statement start")
			} else {
				l.Trace().Caller().Timestamp().Str("scope", scope).Msg("prepare statement start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`prepare statement was success`)
				} else {
					if options.logQuery {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Str("query", query).
							Err(info.Error).
							Str("version", version).
							Msg(`prepare statement failed`)
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Str("version", version).
							Msg(`prepare statement failed`)
					}
				}
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			if options.logQuery {
				l.Trace().Caller().Timestamp().Str("scope", scope).
					Str("query", info.Query).
					Msg("exec start")
			} else {
				l.Trace().Caller().Timestamp().Str("scope", scope).Msg("exec start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`exec was success`)
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Str("query", query).
							Err(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							Str("version", version).
							Msg(`exec failed`)
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							Str("version", version).
							Msg(`exec failed`)
					}
				}
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			if options.logQuery {
				l.Trace().Caller().Timestamp().Str("scope", scope).
					Str("query", info.Query).
					Msg("query start")
			} else {
				l.Trace().Caller().Timestamp().Str("scope", scope).Msg("query start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`query was success`)
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Str("query", query).
							Err(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							Str("version", version).
							Msg(`exec failed`)
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							Str("version", version).
							Msg(`exec failed`)
					}
				}
			}
		}
	}
	if details&trace.DatabaseSQLTxEvents != 0 {
		//nolint:govet
		scope := scope + `.tx`
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("commit start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`committed`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`commit failed`)
				}
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("rollback start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`rollbacked`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`rollback failed`)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		scope := scope + `.stmt`
		t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			l.Trace().Caller().Timestamp().Str("scope", scope).Msg("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				if info.Error == nil {
					l.Trace().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`closed`)
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Str("version", version).
						Msg(`close failed`)
				}
			}
		}
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			if options.logQuery {
				l.Trace().Caller().Timestamp().Str("scope", scope).
					Str("query", info.Query).
					Msg("exec start")
			} else {
				l.Trace().Caller().Timestamp().Str("scope", scope).Msg("exec start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`exec was success`)
				} else {
					if options.logQuery {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Str("query", query).
							Err(info.Error).
							Str("version", version).
							Msg(`exec failed`)
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Str("version", version).
							Msg(`exec failed`)
					}
				}
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			if options.logQuery {
				l.Trace().Caller().Timestamp().Str("scope", scope).
					Str("query", info.Query).
					Msg("query start")
			} else {
				l.Trace().Caller().Timestamp().Str("scope", scope).Msg("query start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).
						Dur("latency", time.Since(start)).
						Msg(`query was success`)
				} else {
					if options.logQuery {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Str("query", query).
							Err(info.Error).
							Str("version", version).
							Msg(`query failed`)
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Str("version", version).
							Msg(`query failed`)
					}
				}
			}
		}
	}
	return t
}
