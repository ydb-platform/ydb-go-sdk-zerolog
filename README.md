# zerolog

zerolog package helps to create ydb-go-sdk traces with logging driver events with zerolog

## Usage
```go
import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
)

func main() {
	// init your zerolog.Logger
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbZerolog.WithTraces(
			&log,
			trace.DetailsAll,
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	// work with db
}
```
