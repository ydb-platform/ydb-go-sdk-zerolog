package zerolog

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(log *zerolog.Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		scope := "ydb.discovery"
		t.OnDiscover = func(info trace.DiscoverStartInfo) func(trace.DiscoverDoneInfo) {
			log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
				Msg("try to discover")
			start := time.Now()
			return func(info trace.DiscoverDoneInfo) {
				if info.Error == nil {
					log.Debug().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Strs("endpoints", info.Endpoints).
						Msg("discover finished")
				} else {
					log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Dur("latency", time.Since(start)).
						Msg("discover failed")
				}
			}
		}
	}
	return t
}