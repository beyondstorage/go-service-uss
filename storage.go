package uss

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/upyun/go-sdk/v3/upyun"

	"github.com/aos-dev/go-storage/v3/pkg/headers"
	"github.com/aos-dev/go-storage/v3/pkg/iowrap"
	. "github.com/aos-dev/go-storage/v3/types"
)

const (
	// iterEnd indicates the last page of list
	// more detail at: http://docs.upyun.com/api/rest_api/#_13
	iterEnd = "g2gCZAAEbmV4dGQAA2VvZg"

	headerListIter  = "X-List-Iter"
	headerListLimit = "X-List-Limit"
)

// delete implements Storager.Delete
//
// USS requires a short time between PUT and DELETE, or we will get this error:
// DELETE 429 {"msg":"concurrent put or delete","code":42900007,"id":"xxx"}
//
// Due to this problem, uss can't pass the storager integration tests.
func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	err = s.bucket.Delete(&upyun.DeleteObjectConfig{
		Path: rp,
	})
	if err != nil {
		return err
	}
	return
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		// 50 is the recommended value in SDK
		// see more details at: https://github.com/upyun/go-sdk/blob/master/upyun/rest.go#L560
		limit:  "50",
		prefix: s.getAbsPath(path),
	}
	if opt.HasContinuationToken {
		input.iter = opt.ContinuationToken
	}

	var nextFn NextObjectFunc

	switch {
	case opt.ListMode.IsDir():
		nextFn = s.nextObjectPageByDir
	case opt.ListMode.IsPrefix():
		nextFn = s.nextObjectPageByPrefix
	default:
		return nil, fmt.Errorf("invalid list mode")
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) metadata(ctx context.Context, opt pairStorageMetadata) (meta *StorageMeta, err error) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta, nil
}

func (s *Storage) nextObjectPageByDir(ctx context.Context, page *ObjectPage) (err error) {
	input := page.Status.(*objectPageStatus)

	header := make(map[string]string)
	header[headerListLimit] = input.limit
	header[headerListIter] = input.iter

	// err could be updated in multiple goroutines, add explict lock to protect it.
	var errlock sync.Mutex

	// USS SDK will close this channel in List
	ch := make(chan *upyun.FileInfo, 1)

	go func() {
		xerr := s.bucket.List(&upyun.GetObjectsConfig{
			Path:         input.prefix,
			ObjectsChan:  ch,
			MaxListLevel: 1, // 1 means not recursive
			Headers:      header,
		})

		errlock.Lock()
		defer errlock.Unlock()
		err = xerr
	}()

	for v := range ch {
		if v.IsDir {
			o := s.newObject(true)
			o.ID = v.Name
			o.Path = s.getRelPath(v.Name)
			o.Mode |= ModeDir
			// v.Meta means all the k-v in header with key which has prefix `x-upyun-meta-`
			// so we consider it as user's metadata
			// see more details at: https://github.com/upyun/go-sdk/blob/master/upyun/fileinfo.go#L39
			o.SetUserMetadata(v.Meta)

			page.Data = append(page.Data, o)
			continue
		}

		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if header[headerListIter] == iterEnd {
		return IterateDone
	}

	input.iter = header[headerListIter]
	return nil
}

func (s *Storage) nextObjectPageByPrefix(ctx context.Context, page *ObjectPage) (err error) {
	input := page.Status.(*objectPageStatus)

	header := make(map[string]string)
	header[headerListLimit] = input.limit
	header[headerListIter] = input.iter

	// err could be updated in multiple goroutines, add explict lock to protect it.
	var errlock sync.Mutex

	// USS SDK will close this channel in List
	ch := make(chan *upyun.FileInfo, 1)

	go func() {
		xerr := s.bucket.List(&upyun.GetObjectsConfig{
			Path:         input.prefix,
			ObjectsChan:  ch,
			MaxListLevel: -1, // -1 means recursive
			Headers:      header,
		})

		errlock.Lock()
		defer errlock.Unlock()
		err = xerr
	}()

	for v := range ch {
		if v.IsDir {
			continue
		}

		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if header[headerListIter] == iterEnd {
		return IterateDone
	}

	input.iter = header[headerListIter]
	return nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)

	config := &upyun.GetObjectConfig{
		Path:   rp,
		Writer: w,
	}

	if opt.HasIoCallback {
		config.Writer = iowrap.CallbackWriter(w, opt.IoCallback)
	}

	f, err := s.bucket.Get(config)

	if err != nil {
		return 0, err
	}

	return f.Size, nil
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)

	output, err := s.bucket.GetInfo(rp)
	if err != nil {
		return nil, err
	}

	return s.formatFileObject(output)
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	rp := s.getAbsPath(path)

	cfg := &upyun.PutObjectConfig{
		Path:   rp,
		Reader: r,
		Headers: map[string]string{
			headers.ContentLength: strconv.FormatInt(size, 10),
		},
	}

	err = s.bucket.Put(cfg)
	if err != nil {
		return
	}
	return size, nil
}
