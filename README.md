# zerolog

zerolog package helps to create ydb-go-sdk traces with logging driver events with zerolog

## Usage
```go
import (
    "fmt"
    "sync/mutex"
    "time"

    "go.uber.org/zap"

    "github.com/ydb-platform/ydb-go-sdk/v3"

    ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
)

func main() {
	// init your zap.Logger
	log = zerolog.New(os.Stdout).With().Timestamp().Logger()
	
    db, err := ydb.New(
        context.Background(),
		ydb.MustConnectionString(connection),
		ydb.WithTraceDriver(ydbZerolog.Driver(
			&log,
			ydbZerolog.DetailsAll,
		)),
		ydb.WithTraceTable(ydbZerolog.Table(
			&log,
			ydbZerolog.DetailsAll,
		)),
	)
    // work with db
}
```
