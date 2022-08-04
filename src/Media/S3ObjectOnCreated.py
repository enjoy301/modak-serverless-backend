import os
import boto3
import json
from PIL import Image
import shutil
from botocore.client import Config
from datetime import datetime
import pymysql


s3 = boto3.resource('s3')
lambda_client = boto3.client('lambda')
s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))


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


# 메신저뷰에서 띄울 미디어를 위한 presigned url for download 생성
def generate_signed_url(key):
    url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': "chatapp-private",
            'Key': key
        },
        ExpiresIn=86400
    )

    return url


# 메시지 보내는 람다 trigger
def invoke_message_send_lambda(file_list, object_metadata, time_now_str):
    message = {
        'body': {
            'family_id': object_metadata['family_id'],
            'user_id': object_metadata['user_id'],
            'content': '',
            'send_at': time_now_str,
            'metadata': {
                'type_code': '',
                'url': '',
                'count': 1
            }
        }
    }
    is_first_image = True

    for file_name, file_ext, file in file_list:
        new_key = object_metadata['family_id'] + '/' + time_now_str + '_' + file

        if file_ext in ['png', 'PNG', 'jpg', 'JPG', 'jpeg', 'JPEG']:
            if not is_first_image:
                continue

            is_first_image = False

            url = generate_signed_url(new_key)

            message['body']['metadata']['url'] = url
            message['body']['metadata']['count'] = object_metadata['image_count']
            message['body']['metadata']['type_code'] = 'image'
            message['body']['metadata']['key'] = new_key

        elif file_ext in ['mov', 'MOV', 'mp4', 'MP4']:
            url = generate_signed_url(new_key)

            message['body']['metadata']['url'] = url
            message['body']['metadata']['count'] = 1
            message['body']['metadata']['type_code'] = 'video'
            message['body']['metadata']['key'] = new_key

        lambda_client.invoke(
            FunctionName='test-put-message',
            Payload=json.dumps(message)
        )


def unzip_object(zip_file_path):
    dir_path = "/tmp/unzip"
    shutil.unpack_archive(zip_file_path, dir_path, 'zip')

    file_list = []
    for (root, _, files) in os.walk(dir_path):
        if '__MACOSX' not in root:
            for file in files:
                # (1, png, 1.png)
                file_name, file_ext = file.split('.')
                file_list.append((file_name, file_ext, file))
    file_list.sort()

    return file_list


# 압축해제한 파일 s3에 업로드
def upload_file_list(file_list, family_id, time_now_str):
    for _, _, file in file_list:
        new_key = family_id + '/' + time_now_str + '_' + file
        s3.meta.client.upload_file('/tmp/unzip/' + file, 'chatapp-private', new_key)

    return


def get_db_secrets():
    secrets = {
        'db': os.environ.get('db'),
        'host': os.environ.get('host'),
        'password': os.environ.get('password'),
        'port': int(os.environ.get('port')),
        'username': os.environ.get('username')
    }

    return secrets


def insert_into_table(file_list, object_metadata, time_now_str):
    secrets = get_db_secrets()

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    sql = "insert into media_temp (user_id, family_id, `key`, `order`, send_at) VALUES"

    family_id = object_metadata['family_id']
    user_id = object_metadata['user_id']

    for idx, (file_name, file_ext, file) in enumerate(file_list):
        if idx == 0:
            sql += f" ({user_id}, {family_id}, '{family_id}/{time_now_str}_{file}', '{file_name}', '{datetime.strptime(time_now_str, '%Yy%mm%dd%Hh%Mm%Ss%f')}')"
        else:
            sql += f", ({user_id}, {family_id}, '{family_id}/{time_now_str}_{file}', '{file_name}', '{datetime.strptime(time_now_str, '%Yy%mm%dd%Hh%Mm%Ss%f')}')"

    cursor.execute(sql)
    connection.commit()

    return


def lambda_handler(event, context):
    object_key = event['Records'][0]['s3']['object']['key']
    time_now_str = datetime.now().strftime('%Yy%mm%dd%Hh%Mm%Ss%f')

    object_metadata = s3.Object('chatapp-private', object_key).metadata

    zip_file_path = '/tmp/' + object_key.split('/')[1]
    s3.meta.client.download_file('chatapp-private', object_key, zip_file_path)

    file_list = unzip_object(zip_file_path)

    upload_file_list(file_list, object_metadata['family_id'], time_now_str)

    invoke_message_send_lambda(file_list, object_metadata, time_now_str)
    insert_into_table(file_list, object_metadata, time_now_str)

    return {
        'statusCode': 200
    }
