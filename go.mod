module github.com/ydb-platform/ydb-go-sdk-zerolog

go 1.16

require (
	github.com/rs/zerolog v1.22.0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.1-rc0
	github.com/zenazn/goji v0.9.0 // indirect
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
