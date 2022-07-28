import json
import boto3
from botocore.client import Config


def lambda_handler(event, context):
    user_id = event['queryStringParameters']['u']
    family_id = event['queryStringParameters']['f']

    s3 = boto3.client('s3', config=Config(signature_version='s3v4'))

    key = str(family_id) + "/" + str(user_id) + "/" + "${filename}"
    url = s3.generate_presigned_post(
        Bucket="chatapp-private",
        Key=key
    )

    return {
        'statusCode': 200,
        'body': json.dumps(url)
    }
