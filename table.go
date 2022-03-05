package zerolog

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with zerolog logging
func Table(log *zerolog.Logger, details trace.Details) trace.Table {
	scope := "ydb.table"
	t := trace.Table{}
	if details&trace.TablePoolRetryEvents != 0 {
		scope := scope + ".retry"
		do := scope + ".do"
		doTx := scope + ".doTx"
		t.OnPoolDo = func(info trace.PoolDoStartInfo) func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
			idempotent := info.Idempotent
			log.Debug().Caller().Timestamp().Str("scope", do).Str("version", version).
				Bool("idempotent", idempotent).
				Msg("init")
			start := time.Now()
			return func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", do).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Msg("intermediate")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", do).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.PoolDoDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", do).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Int("attempts", info.Attempts).
							Msg("finish")
					} else {
						log.Error().Caller().Timestamp().Str("scope", do).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Int("attempts", info.Attempts).
							Err(info.Error).
							Msg("finish")
					}
				}
			}
		}
		t.OnPoolDoTx = func(info trace.PoolDoTxStartInfo) func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
			idempotent := info.Idempotent
			log.Debug().Caller().Timestamp().Str("scope", doTx).Str("version", version).
				Bool("idempotent", idempotent).
				Msg("init")
			start := time.Now()
			return func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", doTx).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Msg("intermediate")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", doTx).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("idempotent", idempotent).
						Err(info.Error).
						Msg("intermediate failed")
				}
				return func(info trace.PoolDoTxDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", doTx).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
							Int("attempts", info.Attempts).
							Msg("finish")
					} else {
						log.Error().Caller().Timestamp().Str("scope", doTx).Str("version", version).
							Dur("latency", time.Since(start)).
							Bool("idempotent", idempotent).
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
			t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("try to create")
				start := time.Now()
				return func(info trace.SessionNewDoneInfo) {
					if info.Error == nil {
						if info.Session != nil {
							log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", info.Session.ID()).
								Msg("created")
						} else {
							log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Msg("not created")
						}
					} else {
						log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Msg("create failed")
					}
				}
			}
			t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
				session := info.Session
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("try to delete")
				start := time.Now()
				return func(info trace.SessionDeleteDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("deleted")
					} else {
						log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("delete failed")
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				session := info.Session
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("keep-aliving")
				start := time.Now()
				return func(info trace.KeepAliveDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("keep-alived")
					} else {
						log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
					info trace.PrepareDataQueryStartInfo,
				) func(
					trace.PrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Str("query", query).
						Msg("preparing")
					start := time.Now()
					return func(info trace.PrepareDataQueryDoneInfo) {
						if info.Error == nil {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("query", query).
								Str("yql", info.Result.String()).
								Msg("prepared")
						} else {
							log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
					info trace.ExecuteDataQueryStartInfo,
				) func(
					trace.ExecuteDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Str("yql", query.String()).
						Str("params", params.String()).
						Msg("executing")
					start := time.Now()
					return func(info trace.ExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
							log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
					info trace.SessionQueryStreamExecuteStartInfo,
				) func(
					intermediateInfo trace.SessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.SessionQueryStreamExecuteDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Str("yql", query.String()).
						Str("params", params.String()).
						Msg("executing")
					start := time.Now()
					return func(
						info trace.SessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.SessionQueryStreamExecuteDoneInfo,
					) {
						if info.Error == nil {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("yql", query.String()).
								Str("params", params.String()).
								Msg("intermediate")
						} else {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Str("yql", query.String()).
								Str("params", params.String()).
								Err(info.Error).
								Msg("intermediate failed")
						}
						return func(info trace.SessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
									Dur("latency", time.Since(start)).
									Str("id", session.ID()).
									Str("status", session.Status()).
									Str("yql", query.String()).
									Str("params", params.String()).
									Err(info.Error).
									Msg("executed")
							} else {
								log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
					info trace.SessionQueryStreamReadStartInfo,
				) func(
					trace.SessionQueryStreamReadIntermediateInfo,
				) func(
					trace.SessionQueryStreamReadDoneInfo,
				) {
					session := info.Session
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Msg("reading")
					start := time.Now()
					return func(
						info trace.SessionQueryStreamReadIntermediateInfo,
					) func(
						trace.SessionQueryStreamReadDoneInfo,
					) {
						if info.Error == nil {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Msg("intermediate")
						} else {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Str("id", session.ID()).
								Str("status", session.Status()).
								Err(info.Error).
								Msg("intermediate failed")
						}
						return func(info trace.SessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
									Dur("latency", time.Since(start)).
									Str("id", session.ID()).
									Str("status", session.Status()).
									Msg("read")
							} else {
								log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				session := info.Session
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("beginning")
				start := time.Now()
				return func(info trace.SessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", info.Tx.ID()).
							Msg("began")
					} else {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("begin failed")
					}
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				session := info.Session
				tx := info.Tx
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Str("tx", tx.ID()).
					Msg("committing")
				start := time.Now()
				return func(info trace.SessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Msg("committed")
					} else {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Err(info.Error).
							Msg("commit failed")
					}
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				session := info.Session
				tx := info.Tx
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Str("tx", tx.ID()).
					Msg("try to rollback")
				start := time.Now()
				return func(info trace.SessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Str("tx", tx.ID()).
							Msg("rollback done")
					} else {
						log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
				log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("initializing")
				start := time.Now()
				return func(info trace.PoolInitDoneInfo) {
					log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Int("minSize", info.KeepAliveMinSize).
						Int("maxSize", info.Limit).
						Msg("initialized")
				}
			}
			t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
				log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("closing")
				start := time.Now()
				return func(info trace.PoolCloseDoneInfo) {
					if info.Error == nil {
						log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Msg("closed")
					} else {
						log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Msg("close failed")
					}
				}
			}
		}
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			scope := scope + ".session"
			t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("try to create")
				start := time.Now()
				return func(info trace.PoolSessionNewDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("created")
					} else {
						log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Err(info.Error).
							Msg("created")
					}
				}
			}
			t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
				session := info.Session
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("closing")
				start := time.Now()
				return func(info trace.PoolSessionCloseDoneInfo) {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("id", session.ID()).
						Str("status", session.Status()).
						Msg("closed")
				}
			}
			t.OnPoolStateChange = func(info trace.PooStateChangeInfo) {
				log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Int("size", info.Size).
					Str("event", info.Event).
					Msg("updated")
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				session := info.Session
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Str("id", session.ID()).
					Str("status", session.Status()).
					Msg("putting")
				start := time.Now()
				return func(info trace.PoolPutDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Msg("put")
					} else {
						log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Err(info.Error).
							Msg("put failed")
					}
				}
			}
			t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("getting")
				start := time.Now()
				return func(info trace.PoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("id", session.ID()).
							Str("status", session.Status()).
							Int("attempts", info.Attempts).
							Msg("got")
					} else {
						log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Int("attempts", info.Attempts).
							Err(info.Error).
							Msg("get failed")
					}
				}
			}
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Msg("waiting")
				start := time.Now()
				return func(info trace.PoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Msg("wait done without any significant result")
						} else {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Dur("latency", time.Since(start)).
								Str("id", info.Session.ID()).
								Str("status", info.Session.Status()).
								Msg("wait done")
						}
					} else {
						if info.Session == nil {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
								Err(info.Error).
								Dur("latency", time.Since(start)).
								Msg("wait failed")
						} else {
							log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
