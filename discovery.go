package zerolog

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(log *zerolog.Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		scope := "ydb.discovery"
		t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
			address := info.Address
			log.Info().Caller().Timestamp().Str("scope", scope).
				Str("address", address).
				Msg("try to discover")
			start := time.Now()
			return func(info trace.DiscoveryDiscoverDoneInfo) {
				if info.Error == nil {
					endpoints := make([]string, 0, len(info.Endpoints))
					for _, e := range info.Endpoints {
						endpoints = append(endpoints, e.String())
					}
					log.Info().Caller().Timestamp().Str("scope", scope).
						Str("address", address).
						Dur("latency", time.Since(start)).
						Strs("endpoints", endpoints).
						Msg("discover finished")
				} else {
					log.Error().Caller().Timestamp().Str("scope", scope).Str("version", version).
						Str("address", address).
						Dur("latency", time.Since(start)).
						Msg("discover failed")
				}
			}
		}
	}
	return t
}
