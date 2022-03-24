package zerolog

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"time"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with zerolog logging
func Table(l *zerolog.Logger, details trace.Details) trace.Table {
	scope := "ydb.table"
	t := trace.Table{}
	if details&trace.TableEvents != 0 {
		t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
			l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("initializing")
			start := time.Now()
			return func(info trace.TableInitDoneInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Int("minSize", info.KeepAliveMinSize).
					Int("maxSize", info.Limit).
					Msg("initialized")
			}
		}
		t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
			l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("closing")
			start := time.Now()
			return func(info trace.TableCloseDoneInfo) {
				if info.Error == nil {
					l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Msg("closed")
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("close failed")
				}
			}
		}
		do := scope + ".do"
		doTx := scope + ".doTx"
		createSession := scope + ".createSession"
		t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", createSession).Str("version", version).
				Msg("init")
			start := time.Now()
			return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", createSession).Str("version", version).
						Dur("latency", time.Since(start)).
						Msg("intermediate")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", createSession).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.TableCreateSessionDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", createSession).Str("version", version).
							Dur("latency", time.Since(start)).
							Int("attempts", info.Attempts).
							Msg("finish")
					} else {
						l.Error().Caller().Timestamp().Str("scope", createSession).Str("version", version).
							Dur("latency", time.Since(start)).
							Int("attempts", info.Attempts).
							Str("id", info.Session.ID()).
							Str("status", info.Session.Status()).
							Err(info.Error).
							Msg("finish")
					}
				}
			}
		}
		t.OnDo = func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			idempotent := info.Idempotent
			l.Debug().Caller().Timestamp().Str("scope", do).Str("version", version).
				Bool("idempotent", idempotent).
				Msg("init")
			start := time.Now()
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", do).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Msg("intermediate")
				} else {
					f := l.Warn()
					if !ydb.IsYdbError(info.Error) {
						f = l.Debug()
					}
					m := retry.Check(info.Error)
					f.Caller().Timestamp().Str("scope", do).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Bool("retryable", m.MustRetry(idempotent)).
						Bool("deleteSession", m.MustDeleteSession()).
						Int64("code", m.StatusCode()).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.TableDoDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", do).Str("version", version).
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
						f.Caller().Timestamp().Str("scope", do).Str("version", version).
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
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			idempotent := info.Idempotent
			l.Debug().Caller().Timestamp().Str("scope", doTx).Str("version", version).
				Bool("idempotent", idempotent).
				Msg("init")
			start := time.Now()
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", doTx).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Msg("intermediate")
				} else {
					f := l.Warn()
					if !ydb.IsYdbError(info.Error) {
						f = l.Debug()
					}
					m := retry.Check(info.Error)
					f.Caller().Timestamp().Str("scope", doTx).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Bool("retryable", m.MustRetry(idempotent)).
						Bool("deleteSession", m.MustDeleteSession()).
						Int64("code", m.StatusCode()).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.TableDoTxDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", doTx).Str("version", version).
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
						f.Caller().Timestamp().Str("scope", doTx).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Bool("retryable", m.MustRetry(idempotent)).
							Bool("deleteSession", m.MustDeleteSession()).
							Int64("code", m.StatusCode()).
							Int("attempts", info.Attempts).
							Err(info.Error).
							Msg("finish")
					}
				}
			}
		}
	}
	if details&trace.TableSessionEvents != 0 {
		scope := scope + ".session"
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("try to create")
				start := time.Now()
				return func(info trace.TableSessionNewDoneInfo) {
					if info.Error == nil {
						if info.Session != nil {
							l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", info.Session.ID()).
								Msg("created")
						} else {
							l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Msg("not created")
						}
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Msg("create failed")
					}
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				session := info.Session
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("try to delete")
				start := time.Now()
				return func(info trace.TableSessionDeleteDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("deleted")
					} else {
						l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("delete failed")
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				session := info.Session
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("keep-aliving")
				start := time.Now()
				return func(info trace.TableKeepAliveDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("keep-alived")
					} else {
						l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("keep-alive failed")
					}
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			scope := scope + ".query"
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				scope := scope + ".invoke"
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Str("query", query).
						Msg("preparing")
					start := time.Now()
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						if info.Error == nil {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("query", query).
								Str("yql", info.Result.String()).
								Msg("prepared")
						} else {
							l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("query", query).
								Err(info.Error).
								Msg("prepare failed")
						}
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.TableExecuteDataQueryStartInfo,
				) func(
					trace.TableExecuteDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Str("yql", query.String()).
						Str("params", params.String()).
						Msg("executing")
					start := time.Now()
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("tx", tx.ID()).
								Str("yql", query.String()).
								Str("params", params.String()).
								Bool("prepared", info.Prepared).
								AnErr("resultErr", info.Result.Err()).
								Msg("executed")
						} else {
							l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("yql", query.String()).
								Str("params", params.String()).
								Bool("prepared", info.Prepared).
								Err(info.Error).
								Msg("execute failed")
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
				scope := scope + ".stream"
				t.OnSessionQueryStreamExecute = func(
					info trace.TableSessionQueryStreamExecuteStartInfo,
				) func(
					intermediateInfo trace.TableSessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamExecuteDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Str("yql", query.String()).
						Str("params", params.String()).
						Msg("executing")
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						if info.Error == nil {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("yql", query.String()).
								Str("params", params.String()).
								Msg("intermediate")
						} else {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("yql", query.String()).
								Str("params", params.String()).
								Err(info.Error).
								Msg("intermediate failed")
						}
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
									Dur("latency", time.Since(start)).
									Str("id", session.ID()).
									Str("status", session.Status()).
									Str("yql", query.String()).
									Str("params", params.String()).
									Err(info.Error).
									Msg("executed")
							} else {
								l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
									Dur("latency", time.Since(start)).
									Str("id", session.ID()).
									Str("status", session.Status()).
									Str("yql", query.String()).
									Str("params", params.String()).
									Err(info.Error).
									Msg("execute failed")
							}
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.TableSessionQueryStreamReadStartInfo,
				) func(
					trace.TableSessionQueryStreamReadIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamReadDoneInfo,
				) {
					session := info.Session
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Msg("reading")
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						if info.Error == nil {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Msg("intermediate")
						} else {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Err(info.Error).
								Msg("intermediate failed")
						}
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
									Dur("latency", time.Since(start)).
									Str("id", session.ID()).
									Str("status", session.Status()).
									Msg("read")
							} else {
								l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
									Dur("latency", time.Since(start)).
									Str("id", session.ID()).
									Str("status", session.Status()).
									Err(info.Error).
									Msg("read failed")
							}
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			scope := scope + ".transaction"
			t.OnSessionTransactionBegin = func(info trace.TableSessionTransactionBeginStartInfo) func(trace.TableSessionTransactionBeginDoneInfo) {
				session := info.Session
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("beginning")
				start := time.Now()
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", info.Tx.ID()).
							Msg("began")
					} else {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("begin failed")
					}
				}
			}
			t.OnSessionTransactionCommit = func(info trace.TableSessionTransactionCommitStartInfo) func(trace.TableSessionTransactionCommitDoneInfo) {
				session := info.Session
				tx := info.Tx
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Str("tx", tx.ID()).
					Msg("committing")
				start := time.Now()
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Msg("committed")
					} else {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Err(info.Error).
							Msg("commit failed")
					}
				}
			}
			t.OnSessionTransactionRollback = func(info trace.TableSessionTransactionRollbackStartInfo) func(trace.TableSessionTransactionRollbackDoneInfo) {
				session := info.Session
				tx := info.Tx
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Str("tx", tx.ID()).
					Msg("try to rollback")
				start := time.Now()
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Msg("rollback done")
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Err(info.Error).
							Msg("rollback failed")
					}
				}
			}
		}
	}
	if details&trace.TablePoolEvents != 0 {
		scope := scope + ".pool"
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			scope := scope + ".session"
			t.OnPoolSessionNew = func(info trace.TablePoolSessionNewStartInfo) func(trace.TablePoolSessionNewDoneInfo) {
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("try to create")
				start := time.Now()
				return func(info trace.TablePoolSessionNewDoneInfo) {
					if info.Error == nil {
						session := info.Session
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("created")
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Msg("created")
					}
				}
			}
			t.OnPoolSessionClose = func(info trace.TablePoolSessionCloseStartInfo) func(trace.TablePoolSessionCloseDoneInfo) {
				session := info.Session
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("closing")
				start := time.Now()
				return func(info trace.TablePoolSessionCloseDoneInfo) {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Msg("closed")
				}
			}
			t.OnPoolStateChange = func(info trace.TablePooStateChangeInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Int("size", info.Size).
					Str("event", info.Event).
					Msg("updated")
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				session := info.Session
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("putting")
				start := time.Now()
				return func(info trace.TablePoolPutDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("put")
					} else {
						l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("put failed")
					}
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("getting")
				start := time.Now()
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Int("attempts", info.Attempts).
							Msg("got")
					} else {
						l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Int("attempts", info.Attempts).
							Err(info.Error).
							Msg("get failed")
					}
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("waiting")
				start := time.Now()
				return func(info trace.TablePoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Msg("wait done without any significant result")
						} else {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", info.Session.ID()).
								Str("status", info.Session.Status()).
								Msg("wait done")
						}
					} else {
						if info.Session == nil {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Err(info.Error).
								Dur("latency", time.Since(start)).
								Msg("wait failed")
						} else {
							l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Err(info.Error).
								Dur("latency", time.Since(start)).
								Str("id", info.Session.ID()).
								Str("status", info.Session.Status()).
								Msg("wait failed")
						}
					}
				}
			}
		}
	}
	return t
}
