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
	
    db, err := ydb.Open(
        context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbZerolog.WithTraces(
			&log,
			ydbZerolog.DetailsAll,
		),
	)
    // work with db
}
```
