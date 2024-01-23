package minoss

var oss *MinOSS

func init() {
	oss = NewMinOSS(Option{
		Endpoint:  "http://127.0.0.1:9000",
		AccessKey: "minio",
		SecretKey: "minio",
		Bucket:    "test",
		Region:    RegionCNNorth1,
	})
}
