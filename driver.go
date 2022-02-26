package zerolog

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with zerol lging
func Driver(l *zerolog.Logger, details trace.Details) trace.Driver {
	scope := "ydb.driver"
	t := trace.Driver{}
	if details&trace.DriverNetEvents != 0 {
		scope := scope + ".net"
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to read")
			start := time.Now()
			return func(info trace.NetReadDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Int("received", info.Received).
						Msg("read")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Int("received", info.Received).
						Err(info.Error).
						Msg("read failed")
				}
			}
		}
		t.OnNetWrite = func(info trace.NetWriteStartInfo) func(trace.NetWriteDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to write")
			start := time.Now()
			return func(info trace.NetWriteDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Int("sent", info.Sent).
						Msg("wrote")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Int("sent", info.Sent).
						Err(info.Error).
						Msg("write failed")
				}
			}
		}
		t.OnNetDial = func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to dial")
			start := time.Now()
			return func(info trace.NetDialDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Msg("dialed")
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Err(info.Error).
						Msg("dial failed")
				}
			}
		}
		t.OnNetClose = func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to close")
			start := time.Now()
			return func(info trace.NetCloseDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).Str("version", version).
						Str("address", address).
						Msg("closed")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).Str("version", version).
						Str("address", address).
						Err(info.Error).
						Msg("close failed")
				}
			}
		}
	}
	if details&trace.DriverCoreEvents != 0 {
		scope := scope + ".core"
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().
				Caller().
				Timestamp().
				Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("try to take conn")
			start := time.Now()
			return func(info trace.ConnTakeDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Msg("conn took")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Err(info.Error).
						Msg("conn take failed")
				}
			}
		}
		t.OnConnUsagesChange = func(info trace.ConnUsagesChangeInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", info.Endpoint.Address()).
				Bool("localDC", info.Endpoint.LocalDC()).
				Str("location", info.Endpoint.Location()).
				Time("lastUpdated", info.Endpoint.LastUpdated()).
				Int("usages", info.Usages).
				Msg("conn usages changed")
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Str("state before", info.State.String()).
				Msg("conn state change")
			start := time.Now()
			return func(info trace.ConnStateChangeDoneInfo) {
				l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", endpoint.Address()).
					Bool("localDC", endpoint.LocalDC()).
					Str("location", endpoint.Location()).
					Time("lastUpdated", endpoint.LastUpdated()).
					Str("state after", info.State.String()).
					Msg("conn state changed")
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			endpoint := info.Endpoint
			method := string(info.Method)
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Str("method", method).
				Msg("try to invoke")
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Str("method", method).
						Msg("invoked")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Str("method", method).
						Err(info.Error).
						Msg("invoke failed")
				}
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			endpoint := info.Endpoint
			method := string(info.Method)
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Str("method", method).
				Msg("try to streaming")
			start := time.Now()
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Str("method", method).
						Msg("streaming intermediate receive")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Str("method", method).
						Err(info.Error).
						Msg("streaming intermediate receive failed")
				}
				return func(info trace.ConnNewStreamDoneInfo) {
					if info.Error == nil {
						l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("address", endpoint.Address()).
							Bool("localDC", endpoint.LocalDC()).
							Str("location", endpoint.Location()).
							Time("lastUpdated", endpoint.LastUpdated()).
							Str("method", method).
							Msg("streaming finished")
					} else {
						l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("address", endpoint.Address()).
							Bool("localDC", endpoint.LocalDC()).
							Str("location", endpoint.Location()).
							Time("lastUpdated", endpoint.LastUpdated()).
							Str("method", method).
							Err(info.Error).
							Msg("streaming failed")
					}
				}
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		scope := scope + ".cluster"
		t.OnClusterInit = func(info trace.ClusterInitStartInfo) func(trace.ClusterInitDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("init start")
			start := time.Now()
			return func(info trace.ClusterInitDoneInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Msg("init done")
			}
		}
		t.OnClusterClose = func(info trace.ClusterCloseStartInfo) func(trace.ClusterCloseDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("close start")
			start := time.Now()
			return func(info trace.ClusterCloseDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Msg("close done")
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("close failed")
				}
			}
		}
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("try to get conn")
			start := time.Now()
			return func(info trace.ClusterGetDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", info.Endpoint.Address()).
						Bool("local", info.Endpoint.LocalDC()).
						Msg("conn got")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("conn get failed")
				}
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("inserting")
			start := time.Now()
			return func(info trace.ClusterInsertDoneInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", endpoint.Address()).
					Bool("localDC", endpoint.LocalDC()).
					Str("location", endpoint.Location()).
					Time("lastUpdated", endpoint.LastUpdated()).
					Str("state", info.State.String()).
					Msg("inserted")
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("removing")
			start := time.Now()
			return func(info trace.ClusterRemoveDoneInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", endpoint.Address()).
					Bool("localDC", endpoint.LocalDC()).
					Str("location", endpoint.Location()).
					Time("lastUpdated", endpoint.LastUpdated()).
					Str("state", info.State.String()).
					Msg("removed")
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("updating")
			start := time.Now()
			return func(info trace.ClusterUpdateDoneInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", endpoint.Address()).
					Bool("localDC", endpoint.LocalDC()).
					Str("location", endpoint.Location()).
					Time("lastUpdated", endpoint.LastUpdated()).
					Str("state", info.State.String()).
					Msg("updated")
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			endpoint := info.Endpoint
			l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				AnErr("cause", info.Cause).
				Msg("pessimizing")
			start := time.Now()
			return func(info trace.PessimizeNodeDoneInfo) {
				l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", endpoint.Address()).
					Bool("localDC", endpoint.LocalDC()).
					Str("location", endpoint.Location()).
					Time("lastUpdated", endpoint.LastUpdated()).
					Str("state", info.State.String()).
					Msg("pessimized")
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		scope := scope + ".credentials"
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("getting")
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("token", log.Secret(info.Token)).
						Msg("got")
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("get failed")
				}
			}
		}
	}
	return t
}
