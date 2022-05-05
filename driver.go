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
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to read")
			start := time.Now()
			return func(info trace.DriverNetReadDoneInfo) {
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
		t.OnNetWrite = func(info trace.DriverNetWriteStartInfo) func(trace.DriverNetWriteDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to write")
			start := time.Now()
			return func(info trace.DriverNetWriteDoneInfo) {
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
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to dial")
			start := time.Now()
			return func(info trace.DriverNetDialDoneInfo) {
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
		t.OnNetClose = func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			address := info.Address
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to close")
			start := time.Now()
			return func(info trace.DriverNetCloseDoneInfo) {
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
	if details&trace.DriverRepeaterEvents != 0 {
		scope := scope + ".repeater"
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			name := info.Name
			event := info.Event
			l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("name", name).
				Str("event", event).
				Msg("repeater wake up")
			start := time.Now()
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				if info.Error == nil {
					l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("name", name).
						Str("event", event).
						Msg("repeater wake up done")
				} else {
					l.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("name", name).
						Str("event", event).
						Err(info.Error).
						Msg("repeater wake up fail")
				}

			}
		}
	}
	if details&trace.DriverConnEvents != 0 {
		scope := scope + ".conn"
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
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
			return func(info trace.DriverConnTakeDoneInfo) {
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
		t.OnConnUsagesChange = func(info trace.DriverConnUsagesChangeInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", info.Endpoint.Address()).
				Bool("localDC", info.Endpoint.LocalDC()).
				Str("location", info.Endpoint.Location()).
				Time("lastUpdated", info.Endpoint.LastUpdated()).
				Int("usages", info.Usages).
				Msg("conn usages changed")
		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Str("state before", info.State.String()).
				Msg("conn state change")
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
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
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
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
			return func(info trace.DriverConnInvokeDoneInfo) {
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
		t.OnConnNewStream = func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
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
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
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
				return func(info trace.DriverConnNewStreamDoneInfo) {
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
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("try to park")
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Msg("parked")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Err(info.Error).
						Msg("park failed")
				}
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("try to close")
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Msg("closed")
				} else {
					l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", endpoint.Address()).
						Bool("localDC", endpoint.LocalDC()).
						Str("location", endpoint.Location()).
						Time("lastUpdated", endpoint.LastUpdated()).
						Err(info.Error).
						Msg("close failed")
				}
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		scope := scope + ".cluster"
		t.OnClusterInit = func(info trace.DriverClusterInitStartInfo) func(trace.DriverClusterInitDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("init start")
			start := time.Now()
			return func(info trace.DriverClusterInitDoneInfo) {
				l.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Msg("init done")
			}
		}
		t.OnClusterClose = func(info trace.DriverClusterCloseStartInfo) func(trace.DriverClusterCloseDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("close start")
			start := time.Now()
			return func(info trace.DriverClusterCloseDoneInfo) {
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
		t.OnClusterGet = func(info trace.DriverClusterGetStartInfo) func(trace.DriverClusterGetDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("try to get conn")
			start := time.Now()
			return func(info trace.DriverClusterGetDoneInfo) {
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
		t.OnClusterInsert = func(info trace.DriverClusterInsertStartInfo) func(trace.DriverClusterInsertDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("inserting")
			start := time.Now()
			return func(info trace.DriverClusterInsertDoneInfo) {
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
		t.OnClusterRemove = func(info trace.DriverClusterRemoveStartInfo) func(trace.DriverClusterRemoveDoneInfo) {
			endpoint := info.Endpoint
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				Msg("removing")
			start := time.Now()
			return func(info trace.DriverClusterRemoveDoneInfo) {
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
		t.OnPessimizeNode = func(info trace.DriverPessimizeNodeStartInfo) func(trace.DriverPessimizeNodeDoneInfo) {
			endpoint := info.Endpoint
			l.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", endpoint.Address()).
				Bool("localDC", endpoint.LocalDC()).
				Str("location", endpoint.Location()).
				Time("lastUpdated", endpoint.LastUpdated()).
				AnErr("cause", info.Cause).
				Msg("pessimizing")
			start := time.Now()
			return func(info trace.DriverPessimizeNodeDoneInfo) {
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
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			l.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("getting")
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
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
