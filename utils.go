package uss

import (
	"fmt"
	"strings"

	"github.com/upyun/go-sdk/v3/upyun"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/pkg/httpclient"
	"github.com/beyondstorage/go-storage/v4/services"
	typ "github.com/beyondstorage/go-storage/v4/types"
)

// Storage is the uss service.
type Storage struct {
	bucket *upyun.UpYun

	name    string
	workDir string

	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	typ.UnimplementedStorager
	typ.UnimplementedDirer
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf("Storager uss {Name: %s, WorkDir: %s}",
		s.name, s.workDir)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...typ.Pair) (typ.Storager, error) {
	return newStorager(pairs...)
}

func newStorager(pairs ...typ.Pair) (store *Storage, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		}
	}()

	store = &Storage{}

	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return
	}

	cp, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}
	if cp.Protocol() != credential.ProtocolHmac {
		return nil, services.PairUnsupportedError{Pair: ps.WithCredential(opt.Credential)}
	}

	operator, password := cp.Hmac()
	cfg := &upyun.UpYunConfig{
		Bucket:   opt.Name,
		Operator: operator,
		Password: password,
	}
	store.bucket = upyun.NewUpYun(cfg)
	// Set http client
	store.bucket.SetHTTPClient(httpclient.New(opt.HTTPClientOptions))
	store.name = opt.Name
	store.workDir = "/"

	if opt.HasDefaultStoragePairs {
		store.defaultPairs = opt.DefaultStoragePairs
	}
	if opt.HasStorageFeatures {
		store.features = opt.StorageFeatures
	}

	if opt.HasWorkDir {
		store.workDir = opt.WorkDir
	}
	return
}

// ref: https://help.upyun.com/knowledge-base/errno/
func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	fn := func(s string) bool {
		return strings.Contains(err.Error(), `"code": `+s)
	}

	switch {
	case !fn(""):
		// If body is empty
		switch {
		case strings.Contains(err.Error(), "404"):
			return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
		default:
			return fmt.Errorf("%w, %v", services.ErrUnexpected, err)
		}
	case fn("40400001"):
		// 40400001:	file or directory not found
		return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
	case fn("40100017"), fn("40100019"), fn("40300011"):
		// 40100017: user need permission
		// 40100019: account forbidden
		// 40300011: has no permission to delete
		return fmt.Errorf("%w: %v", services.ErrPermissionDenied, err)
	default:
		return fmt.Errorf("%w, %v", services.ErrUnexpected, err)
	}
}

// getAbsPath will calculate object storage's abs path
func (s *Storage) getAbsPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return prefix + path
}

// getRelPath will get object storage's rel path.
func (s *Storage) getRelPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return strings.TrimPrefix(path, prefix)
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

func (s *Storage) formatFileObject(v *upyun.FileInfo) (o *typ.Object, err error) {
	o = s.newObject(true)
	o.ID = v.Name
	o.Path = s.getRelPath(v.Name)

	o.SetContentLength(v.Size)
	o.SetLastModified(v.Time)
	// v.Meta means all the k-v in header with key which has prefix `x-upyun-meta-`
	// so we consider it as user's metadata
	// see more details at: https://github.com/upyun/go-sdk/blob/master/upyun/fileinfo.go#L39
	o.SetUserMetadata(v.Meta)

	if v.MD5 != "" {
		o.SetContentMd5(v.MD5)
	}
	if v.ContentType != "" {
		o.SetContentType(v.ContentType)
	}
	if v.IsDir {
		o.Mode |= typ.ModeDir
	} else {
		o.Mode |= typ.ModeRead
	}

	return o, nil
}

func (s *Storage) newObject(stated bool) *typ.Object {
	return typ.NewObject(s, stated)
}

func checkErrorCode(err error, code int) bool {
	if ae, ok := err.(*upyun.Error); ok {
		return ae.Code == code
	}

	return false
}
