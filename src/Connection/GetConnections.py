import os
import pymysql
import boto3
import json


def get_db_secrets():
    secrets = {
        'db': os.environ.get('db'),
        'host': os.environ.get('host'),
        'password': os.environ.get('password'),
        'port': int(os.environ.get('port')),
        'username': os.environ.get('username'),
        'apigateway_endpoint': os.environ.get('apigateway_endpoint'),
    }

    return secrets


def lambda_handler(event, context):
    secrets = get_db_secrets()
    family_id = int(event['queryStringParameters']['f'])

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    # Websocket 연결중인 user 검색
    cursor.execute(
        f"select id, chat_last_joined, connection_id from user_temp where family_id = {family_id};")
    list_family = cursor.fetchall()

    result = {}
    connection_list = []
    for data in list_family:
        family_dict = {
            'id': data[0],
            'last_joined': data[1].timestamp(),
            'is_joining': data[2] != None
        }
        connection_list.append(family_dict)
    result['connection_data'] = connection_list

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
