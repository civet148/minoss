package minoss

import "fmt"

const (
	EffectAllow = "Allow"
	EffectDeny  = "Deny"
)

type Statement struct {
	Effect    string `json:"Effect"`
	Principal struct {
		AWS []string `json:"AWS"`
	} `json:"Principal"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

type Policy struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

func DefaultPublicPolicy(bucket string) string {
	return fmt.Sprintf(`
{
	    "Version":"2012-10-17",
	    "Statement":[
	        {
	            "Effect":"Allow",
	            "Principal":{
	                "AWS":[
	                    "*"
	                ]
	            },
	            "Action":[
	                "s3:GetBucketLocation",
	                "s3:ListBucket",
	                "s3:ListBucketMultipartUploads"
	            ],
	            "Resource":[
	                "arn:aws:s3:::%s"
	            ]
	        },
	        {
	            "Effect":"Allow",
	            "Principal":{
	                "AWS":[
	                    "*"
	                ]
	            },
	            "Action":[
	                "s3:GetObject",
	                "s3:ListMultipartUploadParts"
	            ],
	            "Resource":[
	                "arn:aws:s3:::%s/*"
	            ]
	        }
	    ]
	}
`, bucket, bucket)
}
