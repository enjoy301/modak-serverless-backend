import os
import json
import pymysql
import boto3


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
    connection_id = event['requestContext']['connectionId']
    user_id = int(event['queryStringParameters']['u'])
    family_id = int(event['queryStringParameters']['f'])

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    # connection_id update
    cursor.execute(f"update user_temp set connection_id='{connection_id}' where id={user_id}")
    connection.commit()

    # Websocket 연결중인 user 검색
    cursor.execute(
        f"select id, chat_last_joined, connection_id from user_temp where family_id = {family_id};")
    list_family = cursor.fetchall()

    ag_client = boto3.client('apigatewaymanagementapi', endpoint_url=secrets['apigateway_endpoint'])

    # payload data 생성
    result = {}
    connection_list = []
    for data in list_family:
        family_dict = {
            'id': data[0],
            'last_joined': data[1].timestamp(),
            'is_joining': data[2] is not None
        }
        connection_list.append(family_dict)
    result['connection_data'] = connection_list

    for data in list_family:
        # websocket 메시지 보내기
        if (data[2] is not None) and (data[2] != connection_id):
            try:
                ag_client.post_to_connection(
                    ConnectionId=data[2],
                    Data=json.dumps(result)
                )
            except Exception as e:
                cursor.execute(f"update user_temp set connection_id=NULL where connection_id='{data[2]}'")
                connection.commit()

    connection.close()

    return {
        'statusCode': 200
    }
