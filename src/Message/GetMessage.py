import os
import json
import pymysql
from datetime import datetime


def get_db_secrets():
    secrets = {
        'db': os.environ.get('db'),
        'host': os.environ.get('host'),
        'password': os.environ.get('password'),
        'port': int(os.environ.get('port')),
        'username': os.environ.get('username'),
    }

    return secrets


def lambda_handler(event, context):
    secrets = get_db_secrets()

    last_id = event["pathParameters"]["id"]
    family_id = event["queryStringParameters"]["f"]
    count = int(event["queryStringParameters"]["c"])

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    # last_id가 0인 경우는 제일 처음 불러올 때
    if last_id == '0':
        cursor.execute(
            f'select id, user_id, content, type_code, send_at, metadata from message_temp where family_id={family_id} LIMIT {count};')
    else:
        cursor.execute(
            f'select id, user_id, content, type_code, send_at, metadata  from message_temp where family_id={family_id} and id<{last_id} LIMIT {count};')

    response_messages = cursor.fetchall()
    connection.close()

    # query 결과를 list of objects로 만들기
    list_message = []
    for message in response_messages:
        message_object = {
            'user_id': message[1],
            'content': message[2],
            'type_code': message[3],
            'send_at': datetime.timestamp(message[4]),
            'metadata': message[5]
        }
        list_message.append(message_object)

    result = {"message": list_message}

    if len(response_messages) != 0:
        result["last_id"] = response_messages[len(response_messages) - 1][0]
    else:
        result["last_id"] = -1

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
