package minoss

import (
	"context"
	"github.com/civet148/log"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"strings"
)

const (
	oneMiB = 1024 * 1024
)
const (
	OctetStream = "application/octet-stream"
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

type StorageUsage struct {
	Used      uint64 `json:"used"`
	Available uint64 `json:"available"`
	Total     uint64 `json:"total"`
}

type StorageDetails struct {
	Offline   uint64        `json:"offline"`
	Available uint64        `json:"available"`
	Online    uint64        `json:"online"`
	Disks     []StorageDisk `json:"disks"`
}

type StorageDisk struct {
	DeviceName        string `json:"device_name"`
	DeviceSN          string `json:"device_sn"`
	Instance          string `json:"instance"`
	TotalCapacity     uint64 `json:"total_capacity"`
	AvailableCapacity uint64 `json:"available_capacity"`
	OK                bool   `json:"ok"`
}

type MinOSS struct {
	mc  *minio.Client
	ac  *madmin.AdminClient
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
		panic(log.Errorf("create oss user client error [%s]", err.Error()))
		return nil
	}
	ac, err := madmin.New(opt.Endpoint, opt.AccessKey, opt.SecretKey, opt.Secure)
	if err != nil {
		panic(log.Errorf("create oss admin client error [%s]", err.Error()))
		return nil
	}
	m := &MinOSS{
		mc:  mc,
		ac:  ac,
		opt: opt,
	}
	return m.initialize()
}

func (m *MinOSS) initialize() *MinOSS {
	if m.opt.Bucket != "" {
		err := m.MakeBucket(context.Background(), m.opt.Bucket)
		if err != nil {
			log.Panic(err.Error())
		}
	}
	return m
}

// UserClient returns minio user client
func (m *MinOSS) UserClient() *minio.Client {
	return m.mc
}

// AdminClient returns minio admin client
func (m *MinOSS) AdminClient() *madmin.AdminClient {
	return m.ac
}

// Option return current OSS option
func (m *MinOSS) Option() Option {
	return m.opt
}

// ListBuckets list all buckets
func (m *MinOSS) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	return m.mc.ListBuckets(ctx)
}

// MakeBucket create a bucket
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

// GetObjectList get bucket all objects
func (m *MinOSS) GetObjectList(ctx context.Context, bucket string) (objects []*minio.ObjectInfo, err error) {
	objects = make([]*minio.ObjectInfo, 0)
	objectCh := m.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: false})

	for object := range objectCh {
		if object.Err != nil {
			log.Warnf("get object list error [%s]", object.Err)
			continue
		}
		objects = append(objects, &object)
	}
	return
}

// SearchObject search objects by bucket and file prefix
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

// UploadObject uploads an object to bucket
func (m *MinOSS) UploadObject(ctx context.Context, bucket, objname string, object io.Reader, options ...minio.PutObjectOptions) (err error) {
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

// UploadObjectFromFile uploads an object from file to bucket
func (m *MinOSS) UploadObjectFromFile(ctx context.Context, bucket, objname, filepath string, options ...minio.PutObjectOptions) (err error) {
	var opts minio.PutObjectOptions
	if len(options) == 0 {
		opts = DefaultPutObjectOptions()
	} else {
		opts = options[0]
	}
	_, err = m.mc.FPutObject(ctx, bucket, objname, filepath, opts)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return
}

// DownloadObject downloads an object
func (m *MinOSS) DownloadObject(ctx context.Context, bucket, objname string, writer io.Writer, options ...minio.GetObjectOptions) (written int64, err error) {
	var reader *minio.Object
	var opts minio.GetObjectOptions
	if len(options) == 0 {
		opts = DefaultGetObjectOptions()
	} else {
		opts = options[0]
	}
	reader, err = m.mc.GetObject(ctx, bucket, objname, opts)
	if err != nil {
		return 0, log.Errorf("get object [%s] from bucket [%s] error [%s]", objname, bucket, err.Error())
	}
	return io.Copy(writer, reader)
}

// DownloadObject2File downloads an object write to file
func (m *MinOSS) DownloadObject2File(ctx context.Context, bucket, objname, filepath string, options ...minio.GetObjectOptions) (err error) {
	var opts minio.GetObjectOptions
	if len(options) == 0 {
		opts = DefaultGetObjectOptions()
	} else {
		opts = options[0]
	}
	err = m.mc.FGetObject(ctx, bucket, objname, filepath, opts)
	if err != nil {
		return log.Errorf("get object [%s] from bucket [%s] error [%s]", objname, bucket, err.Error())
	}
	return nil
}

// RemoveObject remove object from bucket
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

func (m *MinOSS) GetBucketPolicy(ctx context.Context, bucket string) (string, error) {
	return m.mc.GetBucketPolicy(ctx, bucket)
}

func (m *MinOSS) SetBucketPolicy(ctx context.Context, bucket, policy string) error {
	return m.mc.SetBucketPolicy(ctx, bucket, policy)
}

func (m *MinOSS) RemoveBucketPolicy(ctx context.Context, bucket string) error {
	return m.mc.SetBucketPolicy(ctx, bucket, "")
}

// SetBucketPublicPolicy sets the bucket to public read policy aims for file downloads directly from oss endpoint
func (m *MinOSS) SetBucketPublicPolicy(ctx context.Context, bucket string) error {
	policy := DefaultPublicPolicy(bucket)
	err := m.SetBucketPolicy(ctx, bucket, policy)
	if err != nil {
		return log.Errorf(err.Error())
	}
	return nil
}

// AdminGetQuota get bucket quota (MiB)
func (m *MinOSS) AdminGetQuota(ctx context.Context, bucket string) (MiB float64, err error) {
	var quota madmin.BucketQuota
	quota, err = m.ac.GetBucketQuota(ctx, bucket)
	if err != nil {
		return 0, log.Errorf(err.Error())
	}
	return float64(quota.Quota / oneMiB), nil
}

// AdminSetQuota set bucket quota in hard mode (MiB)
func (m *MinOSS) AdminSetQuota(ctx context.Context, bucket string, MiB int64) (err error) {
	err = m.ac.SetBucketQuota(ctx, bucket, &madmin.BucketQuota{Quota: uint64(MiB * oneMiB), Type: madmin.HardQuota})
	if err != nil {
		log.Error(err.Error())
		return
	}
	return
}

// AdminGetBucketsUsage get all buckets usage information
func (m *MinOSS) AdminGetBucketsUsage(ctx context.Context) (buckets map[string]madmin.BucketUsageInfo, err error) {
	var usage madmin.DataUsageInfo
	usage, err = m.ac.DataUsageInfo(ctx)
	if err != nil {
		log.Error(err.Error())
		return
	}
	return usage.BucketsUsage, nil
}

// AdminGetDataUsage get total data usage information
func (m *MinOSS) AdminGetDataUsage(ctx context.Context) (usage madmin.DataUsageInfo, err error) {
	return m.ac.DataUsageInfo(ctx)
}

// AdminAddUser add user
func (m *MinOSS) AdminAddUser(ctx context.Context, accessKey, secretKey string) (err error) {
	return m.ac.AddUser(ctx, accessKey, secretKey)
}

// AdminRemoveUser delete user
func (m *MinOSS) AdminRemoveUser(ctx context.Context, accessKey string) (err error) {
	return m.ac.RemoveUser(ctx, accessKey)
}

// AdminSetPolicy set the policy by access key
func (m *MinOSS) AdminSetPolicy(ctx context.Context, accessKey, policyName string, isGroup bool) (err error) {
	return m.ac.SetPolicy(ctx, policyName, accessKey, isGroup)
}

// AdminSetUserStatus set user status (enabled or disabled)
func (m *MinOSS) AdminSetUserStatus(ctx context.Context, accessKey string, status madmin.AccountStatus) (err error) {
	return m.ac.SetUserStatus(ctx, accessKey, status)
}

// AdminGetServerInfo get server information
func (m *MinOSS) AdminGetServerInfo(ctx context.Context) (info madmin.InfoMessage, err error) {
	return m.ac.ServerInfo(ctx)
}

// AdminGetStorageInfo get storage information
func (m *MinOSS) AdminGetStorageInfo(ctx context.Context) (info madmin.StorageInfo, err error) {
	return m.ac.StorageInfo(ctx)
}

// AdminGetStorageUsage get storage usage information
func (m *MinOSS) AdminGetStorageUsage(ctx context.Context) (usage StorageUsage, err error) {
	var storage madmin.StorageInfo
	storage, err = m.ac.StorageInfo(ctx)
	if err != nil {
		return StorageUsage{}, log.Error(err.Error())
	}
	for _, disk := range storage.Disks {
		usage.Total += disk.TotalSpace
		usage.Used += disk.UsedSpace
		usage.Available += disk.AvailableSpace
	}
	return usage, nil
}

// AdminGetStorageDetails get all instance storage details
func (m *MinOSS) AdminGetStorageDetails() (details map[string]StorageDetails, err error) {
	ctx := context.Background()
	var info madmin.InfoMessage
	info, err = m.ac.ServerInfo(ctx)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	details = make(map[string]StorageDetails)
	for _, s := range info.Servers {
		instance := strings.Split(s.Endpoint, ":")[0]
		var online, offline, available uint64
		sd := StorageDetails{}
		sd.Disks = make([]StorageDisk, 0)
		for _, disk := range s.Disks {
			var ok bool
			available += disk.AvailableSpace
			if disk.State == "ok" {
				online++
				ok = true
			} else {
				offline++
			}
			sd.Disks = append(sd.Disks, StorageDisk{
				DeviceName:        disk.DrivePath,
				DeviceSN:          disk.UUID,
				Instance:          s.Endpoint,
				TotalCapacity:     disk.TotalSpace,
				AvailableCapacity: disk.AvailableSpace,
				OK:                ok,
			})
		}
		sd.Offline = offline
		sd.Online = online
		sd.Available = available
		details[instance] = sd
	}
	return
}
