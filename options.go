package minoss

import "github.com/minio/minio-go/v7"

func DefaultRemoveOptions() minio.RemoveObjectOptions {
	return minio.RemoveObjectOptions{
		ForceDelete:      false,
		GovernanceBypass: true,
	}
}

func DefaultGetObjectOptions() minio.GetObjectOptions {
	return minio.GetObjectOptions{}
}

func DefaultPutObjectOptions() minio.PutObjectOptions {
	return minio.PutObjectOptions{
		ContentType: OctetStream,
	}
}
