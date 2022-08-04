import json
import boto3
from botocore.client import Config


def lambda_handler(event, context):
    key_list = json.loads(event['body'])['list']

    url_list = []
    for key in key_list:
        s3 = boto3.client('s3', config=Config(signature_version='s3v4'))

        url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': "chatapp-private",
                'Key': key
            },
            ExpiresIn=86400
        )

        url_list.append(url)

    result = {'url_list': url_list}
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
