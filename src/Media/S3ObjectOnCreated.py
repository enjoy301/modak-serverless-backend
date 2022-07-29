import boto3
import json

s3 = boto3.resource('s3')


def invoke_message_send_lambda(count, key):
    family_id, user_id, filename = key.split('/')

    if count == 1:
        type_code = "image"
    else:
        type_code = "images"

    message = {
        'body': {
            'family_id': family_id,
            'user_id': user_id,
            'content': key,
            'type_code': type_code,
        }
    }

    lambda_client = boto3.client('lambda')
    lambda_client.invoke(
        FunctionName='test-put-message',
        Payload=json.dumps(message)
    )


def generate_thumbnail():
    pass


def lambda_handler(event, context):
    key = event['Records'][0]['s3']['object']['key']

    media_object = s3.Object('chatapp-private', key)
    count = int(media_object.metadata['count'])
    is_first = media_object.metadata['is_first']

    if is_first:
        invoke_message_send_lambda(count, key)

    generate_thumbnail()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
