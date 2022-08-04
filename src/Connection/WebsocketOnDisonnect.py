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
    connection_id = event['requestContext']['connectionId']

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    cursor.execute(f"select family_id from user_temp where connection_id='{connection_id}'")
    family_id = cursor.fetchall()[0][0]

    # update 쿼리시 예외처리로 commit 필수, 안하면 레코드 잡고 안놔줌
    try:
        cursor.execute(
            f"update user_temp set chat_last_joined=NOW(6), connection_id=NULL where connection_id='{connection_id}'")
        connection.commit()
    except:
        connection.commit()
        return {
            'statusCode': 500
        }


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
        if data[2] is not None:
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
