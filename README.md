# go-services-uss

[UPYUN Storage Service](https://www.upyun.com/products/file-storage) support for [go-storage](https://github.com/beyondstorage/go-storage).

## Notes

**This package has been moved to [go-storage](https://github.com/beyondstorage/go-storage/tree/master/services/uss).**

```shell
go get go.beyondstorage.io/services/uss
```

## Install

```go
go get github.com/beyondstorage/go-service-uss/v2
```

## Usage

```go
import (
	"log"

	_ "github.com/beyondstorage/go-service-uss/v2"
	"github.com/beyondstorage/go-storage/v4/services"
)

func main() {
	store, err := services.NewStoragerFromString("uss://bucket_name/path/to/workdir?credential=hmac:<operator>:<password>&endpoint=https:<domain>")
	if err != nil {
		log.Fatal(err)
	}

	// Write data from io.Reader into hello.txt
	n, err := store.Write("hello.txt", r, length)
}
```

- See more examples in [go-storage-example](https://github.com/beyondstorage/go-storage-example).
- Read [more docs](https://beyondstorage.io/docs/go-storage/services/uss) about go-service-uss.
