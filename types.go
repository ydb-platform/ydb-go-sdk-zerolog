package zerolog

import (
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	version = func() string {
		_, version := path.Split(ydb.Version)
		return version
	}()
)
