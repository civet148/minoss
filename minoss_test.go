package minoss

import (
	"context"
	"github.com/civet148/log"
	"os"
	"testing"
)

const (
	testFileName     = "README.md"
	tmpFileName      = "README.md.downloading"
	minossBucketName = "minoss"
)

var oss *MinOSS
var ctx = context.Background()

func init() {
	oss = NewMinOSS(Option{
		Endpoint:  "play.min.io",
		AccessKey: "Q3AM3UQ867SPQQA43P2F",
		SecretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
		Bucket:    minossBucketName,
		Region:    RegionUSEast1,
		Secure:    true,
	})
}

func TestListBuckets(t *testing.T) {
	buckets, err := oss.ListBuckets(ctx)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	log.Json("buckets", buckets)
}

func TestUploadObject(t *testing.T) {
	file, err := os.Open(testFileName)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	err = oss.UploadObject(ctx, minossBucketName, testFileName, file)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	objects, err := oss.GetObjectList(ctx, minossBucketName)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	log.Json("objects", objects)
}

func TestDownloadObject(t *testing.T) {
	file, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	n, err := oss.DownloadObject(ctx, minossBucketName, testFileName, file)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	log.Infof("download file ok, written %v bytes", n)
}

func TestSetBucketPolicy(t *testing.T) {
	policy, err := oss.GetBucketPolicy(ctx, minossBucketName)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	log.Infof("bucket %s current policy [%s]", minossBucketName, policy)
	err = oss.SetBucketPublicPolicy(ctx, minossBucketName)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	policy, err = oss.GetBucketPolicy(ctx, minossBucketName)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	log.Infof("bucket %s new policy [%s]", minossBucketName, policy)
}
