import os
import boto3
import json
from PIL import Image
import shutil
import cv2


s3 = boto3.resource('s3')
lambda_client = boto3.client('lambda')


def generate_image_file_thumbnail(file_path, key):
    filename = file_path.split('/')[3]
    resized_filename = 'resized-' + filename
    resized_image_path = '/tmp/' + resized_filename

    family_id, time, _ = key.split('/')
    image_key = family_id + '/' + time + '/' + filename
    new_key = family_id + '/' + time + '/' + resized_filename

    with Image.open(file_path) as image:
        thumbnail_size = tuple(x / 2 for x in image.size)
        min_size = int(min(thumbnail_size))

        image.thumbnail(thumbnail_size)
        crop_image = image.crop((0, 0, min_size, min_size))
        crop_image.save(resized_image_path)

    s3.meta.client.upload_file(file_path, 'chatapp-private', image_key)
    s3.meta.client.upload_file(resized_image_path, 'chatapp-private', new_key)

    return new_key


def generate_video_file_thumbnail(file_path, key):
    filename = file_path.split('/')[3]
    thumbnail_filename = 'thumbnail-' + filename
    thumbnail_filepath = '/tmp/' + thumbnail_filename

    family_id, time, _ = key.split('/')
    video_key = family_id + '/' + time + '/' + filename
    new_key = family_id + '/' + time + '/' + thumbnail_filename

    cap = cv2.VideoCapture(file_path)
    success, image = cap.read()

    if success:
        cv2.imwrite(thumbnail_filepath, image)

    s3.meta.client.upload_file(file_path, 'chatapp-private', video_key)
    s3.meta.client.upload_file(thumbnail_filepath, 'chatapp-private', new_key)

    return new_key


def invoke_message_send_lambda(file_list, object_metadata, key):
    message = {
        'body': {
            'family_id': object_metadata['family_id'],
            'user_id': object_metadata['user_id'],
            'content': '',
            'type_code': '',
        }
    }

    is_first_image = True

    for media_file_path in file_list:
        file_ext = media_file_path.split('.')[1]

        if file_ext in ['png', 'PNG']:
            thumbnail_key = generate_image_file_thumbnail(media_file_path, key)

            if not is_first_image:
                continue

            is_first_image = False

            message['body']['content'] = thumbnail_key + ' / ' + object_metadata['image_count']
            message['body']['type_code'] = 'image'


        elif file_ext in ['mov', 'MOV']:
            thumbnail_key = generate_video_file_thumbnail(media_file_path, key)

            message['body']['content'] = thumbnail_key
            message['body']['type_code'] = 'video'

        lambda_client.invoke(
            FunctionName='test-put-message',
            Payload=json.dumps(message)
        )


def unzip_object(file_path):
    shutil.unpack_archive(file_path, '/tmp/unzip', 'zip')

    dir_path = "/tmp/unzip"

    file_list = []
    for (root, _, files) in os.walk(dir_path):
        if '__MACOSX' not in root:
            for file in files:
                file_path = os.path.join(root, file)
                file_list.append(file_path)
    file_list.sort()

    return file_list


def insert_table():
    pass


def lambda_handler(event, context):
    key = event['Records'][0]['s3']['object']['key']

    object_metadata = s3.Object('chatapp-private', key).metadata

    file_path = '/tmp/' + key.split('/')[1]
    s3.meta.client.download_file('chatapp-private', key, file_path)

    file_list = unzip_object(file_path)

    invoke_message_send_lambda(file_list, object_metadata, key)

    insert_table()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
