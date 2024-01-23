package minoss

import (
	"context"
	"github.com/civet148/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
)

const (
	OctetStream = "application/octet-stream"
)

type Region string

const (
	RegionCNNorth1     Region = "cn-north-1"     //s3.cn-north-1.amazonaws.com.cn
	RegionCNNorthWest1 Region = "cn-northwest-1" //s3.cn-northwest-1.amazonaws.com.cn
	RegionUSEast1      Region = "us-east-1"      //s3.amazonaws.com
	RegionUSWest1      Region = "us-west-1"      //s3-fips-us-gov-west-1.amazonaws.com
	RegionUSGovWest1   Region = "us-gov-west-1"  //s3-fips-us-gov-west-1.amazonaws.com
)

type Option struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	Region    Region `json:"region"`
	Token     string `json:"token"`
	Secure    bool   `json:"secure"`
}

type MinOSS struct {
	mc  *minio.Client
	opt Option
}

func NewMinOSS(opt Option) *MinOSS {
	if opt.Region == "" {
		opt.Region = RegionUSWest1
	}
	mc, err := minio.New(opt.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opt.AccessKey, opt.SecretKey, opt.Token),
		Secure: opt.Secure,
	})
	if err != nil {
		log.Panic("create OSS client error [%s]", err.Error())
		return nil
	}

	s := &MinOSS{
		mc:  mc,
		opt: opt,
	}
	return s
}

func (m *MinOSS) Client() *minio.Client {
	return m.mc
}

func (m *MinOSS) Option() Option {
	return m.opt
}

func (m *MinOSS) ListBucket(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	return m.mc.ListBuckets(ctx)
}

func (m *MinOSS) MakeBucket(ctx context.Context, bucket string) (err error) {
	var exist bool
	if bucket == "" {
		return log.Error("bucket name is nil")
	}
	if exist, err = m.mc.BucketExists(ctx, bucket); err != nil {
		return log.Error(err.Error())
	}
	if exist {
		return nil
	}
	err = m.mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{
		Region:        string(m.opt.Region),
		ObjectLocking: true,
	})
	if err != nil {
		return log.Error(err)
	}
	return
}

func (m *MinOSS) GetObjectList(ctx context.Context, bucket string) (objects []*minio.ObjectInfo, total int64) {
	objects = make([]*minio.ObjectInfo, 0)
	objectCh := m.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: false})

	for object := range objectCh {
		if object.Err != nil {
			log.Warnf("get object list error [%s]", object.Err)
			continue
		}
		total++
		objects = append(objects, &object)
	}
	return
}

func (m *MinOSS) SearchObject(ctx context.Context, bucket, prefix string, recursive bool) (objects []*minio.ObjectInfo, total int64) {
	objects = make([]*minio.ObjectInfo, 0)
	objectCh := m.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive})
	for object := range objectCh {
		if object.Err != nil {
			log.Warnf("search object error [%s]", object.Err.Error())
			continue
		}
		total++
		objects = append(objects, &object)
	}
	return
}

func (m *MinOSS) UploadFile(ctx context.Context, bucket, objname string, object io.Reader, options ...minio.PutObjectOptions) (err error) {
	var opts minio.PutObjectOptions
	if len(options) == 0 {
		opts = DefaultPutObjectOptions()
	} else {
		opts = options[0]
	}
	_, err = m.mc.PutObject(ctx, bucket, objname, object, -1, opts)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return
}

func (m *MinOSS) DownloadFile(ctx context.Context, bucket, objname string, options ...minio.GetObjectOptions) (reader *minio.Object, err error) {
	var opts minio.GetObjectOptions
	if len(options) == 0 {
		opts = DefaultGetObjectOptions()
	} else {
		opts = options[0]
	}
	reader, err = m.mc.GetObject(ctx, bucket, objname, opts)
	if err != nil {
		return nil, log.Errorf("get object [%s] from bucket [%s] error [%s]", objname, bucket, err.Error())
	}
	return
}

func (m *MinOSS) RemoveObject(ctx context.Context, bucket, objname string, options ...minio.RemoveObjectOptions) (err error) {
	var opts minio.RemoveObjectOptions
	if len(options) == 0 {
		opts = DefaultRemoveOptions()
	} else {
		opts = options[0]
	}
	err = m.mc.RemoveObject(ctx, bucket, objname, opts)
	if err != nil {
		return log.Error(err.Error())
	}
	return
}
