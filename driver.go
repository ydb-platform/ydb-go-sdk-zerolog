package zerolog

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with zap logging
func Driver(log *zerolog.Logger, details trace.Details) trace.Driver {
	scope := "ydb.driver"
	t := trace.Driver{}
	if details&trace.DriverNetEvents != 0 {
		scope := scope + ".net"
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := info.Address
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to read")
			start := time.Now()
			return func(info trace.NetReadDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Int("received", info.Received).
						Msg("read")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to write")
			start := time.Now()
			return func(info trace.NetWriteDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Int("sent", info.Sent).
						Msg("wrote")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to dial")
			start := time.Now()
			return func(info trace.NetDialDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Msg("dialed")
				} else {
					log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Err(info.Error).
						Msg("dial failed")
				}
			}
		}
		t.OnNetClose = func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			address := info.Address
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Msg("try to close")
			start := time.Now()
			return func(info trace.NetCloseDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).Str("version", version).
						Str("address", address).
						Msg("closed")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
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
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug().
				Caller().
				Timestamp().
				Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("dataCenter", dataCenter).
				Msg("try to take conn")
			start := time.Now()
			return func(info trace.ConnTakeDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Bool("dataCenter", dataCenter).
						Msg("conn took")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Bool("dataCenter", dataCenter).
						Err(info.Error).
						Msg("conn take failed")
				}
			}
		}
		t.OnConnRelease = func(info trace.ConnReleaseStartInfo) func(trace.ConnReleaseDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("dataCenter", dataCenter).
				Msg("try to release conn")
			start := time.Now()
			return func(info trace.ConnReleaseDoneInfo) {
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", address).
					Bool("dataCenter", dataCenter).
					Int("locks", info.Lock).
					Msg("conn released")
			}
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("dataCenter", dataCenter).
				Str("state before", info.State.String()).
				Msg("conn state change")
			start := time.Now()
			return func(info trace.ConnStateChangeDoneInfo) {
				log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", address).
					Bool("dataCenter", dataCenter).
					Str("state after", info.State.String()).
					Msg("conn state changed")
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			method := string(info.Method)
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("dataCenter", dataCenter).
				Str("method", method).
				Msg("try to invoke")
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Bool("dataCenter", dataCenter).
						Str("method", method).
						Msg("invoked")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Bool("dataCenter", dataCenter).
						Str("method", method).
						Err(info.Error).
						Msg("invoke failed")
				}
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			method := string(info.Method)
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("dataCenter", dataCenter).
				Str("method", method).
				Msg("try to streaming")
			start := time.Now()
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Bool("dataCenter", dataCenter).
						Str("method", method).
						Msg("streaming intermediate receive")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", address).
						Bool("dataCenter", dataCenter).
						Str("method", method).
						Err(info.Error).
						Msg("streaming intermediate receive failed")
				}
				return func(info trace.ConnNewStreamDoneInfo) {
					if info.Error == nil {
						log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("address", address).
							Bool("dataCenter", dataCenter).
							Str("method", method).
							Msg("streaming finished")
					} else {
						log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
							Dur("latency", time.Since(start)).
							Str("address", address).
							Bool("dataCenter", dataCenter).
							Str("method", method).
							Err(info.Error).
							Msg("streaming failed")
					}
				}
			}
		}
	}
	if details&trace.DriverDiscoveryEvents != 0 {
		scope := scope + ".discovery"
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("try to discover")
			start := time.Now()
			return func(info trace.DiscoveryDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Strs("endpoints", info.Endpoints).
						Msg("discover finished")
				} else {
					log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("discover failed")
				}
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		scope := scope + ".cluster"
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("try to get conn")
			start := time.Now()
			return func(info trace.ClusterGetDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Str("address", info.Endpoint.Address()).
						Bool("local", info.Endpoint.LocalDC()).
						Msg("conn got")
				} else {
					log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Err(info.Error).
						Msg("conn get failed")
				}
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("local", dataCenter).
				Msg("inserting")
			start := time.Now()
			return func(info trace.ClusterInsertDoneInfo) {
				log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", address).
					Bool("local", dataCenter).
					Str("state", info.State.String()).
					Msg("inserted")
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("local", dataCenter).
				Msg("removing")
			start := time.Now()
			return func(info trace.ClusterRemoveDoneInfo) {
				log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", address).
					Bool("local", dataCenter).
					Str("state", info.State.String()).
					Msg("removed")
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("local", dataCenter).
				Msg("updating")
			start := time.Now()
			return func(info trace.ClusterUpdateDoneInfo) {
				log.Info().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", address).
					Bool("local", dataCenter).
					Str("state", info.State.String()).
					Msg("updated")
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Str("address", address).
				Bool("local", dataCenter).
				AnErr("cause", info.Cause).
				Msg("pessimizing")
			start := time.Now()
			return func(info trace.PessimizeNodeDoneInfo) {
				log.Warn().Caller().Timestamp().Str("scope", scope).Str("version", version).
					Dur("latency", time.Since(start)).
					Str("address", address).
					Bool("local", dataCenter).
					Str("state", info.State.String()).
					Err(info.Error).
					Msg("pessimized")
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		scope := scope + ".credentials"
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("getting")
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("token ok", info.TokenOk).
						Msg("got")
				} else {
					log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Bool("token ok", info.TokenOk).
						Err(info.Error).
						Msg("get failed")
				}
			}
		}
	}
	return t
}
