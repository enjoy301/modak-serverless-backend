import json
import boto3
from botocore.client import Config


def lambda_handler(event, context):
    s3 = boto3.client('s3', config=Config(signature_version='s3v4'))

    url = s3.generate_presigned_post(
        Bucket="chatapp-private",
        Key="${filename}",
        Conditions=[
            ["starts-with", "$x-amz-meta-user_id", ""],
            ["starts-with", "$x-amz-meta-family_id", ""],
            ["starts-with", "$x-amz-meta-image_count", ""]
        ],
        ExpiresIn=86400
    )

    return {
        'statusCode': 200,
        'body': json.dumps(url)
    }
