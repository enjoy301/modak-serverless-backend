import boto3
import json
from PIL import Image

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


# 화질 1/4, 정사각형 썸네일 만들고 s3 upload하는 함수
def generate_image_file_thumbnail(key):
    family_id, user_id, filename = key.split('/')
    resized_filename = 'resized-' + filename
    new_key = family_id + '/' + user_id + '/' + resized_filename
    image_path = '/tmp/' + filename
    resized_image_path = '/tmp/' + resized_filename

    s3.meta.client.download_file('chatapp-private', key, image_path)

    with Image.open(image_path) as image:
        thumbnail_size = tuple(x / 2 for x in image.size)
        min_size = int(min(thumbnail_size))

        # 비율 유지하면서 화질 1/4로 줄이기
        image.thumbnail(thumbnail_size)
        # 왼쪽 위를 기준으로 이미지 정사각형으로 자르기
        crop_image = image.crop((0, 0, min_size, min_size))
        crop_image.save(resized_image_path)

    s3.meta.client.upload_file(resized_image_path, 'chatapp-private', new_key)


def lambda_handler(event, context):
    key = event['Records'][0]['s3']['object']['key']

    media_object = s3.Object('chatapp-private', key)
    count = int(media_object.metadata['count'])
    is_first = media_object.metadata['is_first']

    if is_first:
        invoke_message_send_lambda(count, key)

    generate_image_file_thumbnail(key)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
