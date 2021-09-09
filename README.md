[![Build Status](https://github.com/beyondstorage/go-service-uss/workflows/Unit%20Test/badge.svg?branch=master)](https://github.com/beyondstorage/go-service-uss/actions?query=workflow%3A%22Unit+Test%22)
[![License](https://img.shields.io/badge/license-apache%20v2-blue.svg)](https://github.com/Xuanwo/storage/blob/master/LICENSE)
[![](https://img.shields.io/matrix/beyondstorage@go-storage:matrix.org.svg?logo=matrix)](https://matrix.to/#/#beyondstorage@go-storage:matrix.org)

# go-services-uss

[UPYUN Storage Service](https://www.upyun.com/products/file-storage) support for [go-storage](https://github.com/beyondstorage/go-storage).

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
