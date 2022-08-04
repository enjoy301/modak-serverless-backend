import os
import json
import pymysql
import boto3
from datetime import datetime


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
    if type(event["body"]) == str:
        body = json.loads(event["body"])
    else:
        body = event["body"]

    family_id = body["family_id"]
    user_id = body["user_id"]
    content = body["content"]
    metadata = body["metadata"]

    if "send_at" in body:
        send_at = datetime.strptime(body['send_at'], '%Yy%mm%dd%Hh%Mm%Ss%f')
    else:
        send_at = datetime.now()

    secrets = get_db_secrets()

    ag_client = boto3.client('apigatewaymanagementapi', endpoint_url=secrets['apigateway_endpoint'])

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    # 메시지 db에 insert
    cursor.execute(
        f"insert into message_temp (user_id, family_id, content, metadata, send_at) values({user_id}, {family_id}, '{content}', '{json.dumps(metadata)}', '{send_at}');")
    connection.commit()

    # Websocket 연결중인 user 검색
    cursor.execute(
        f"select id, connection_id from user_temp where family_id = {family_id} and connection_id is not null;")
    list_family_on_connecting = cursor.fetchall()

    # 메시지 객체 생성
    message = body
    del message['family_id']
    message['send_at'] = send_at.timestamp()
    result = {'message_data': message}

    list_connection_loss = []
    for member in list_family_on_connecting:
        try:
            # websocket 메시지 보내기
            ag_client.post_to_connection(
                ConnectionId=member[1],
                Data=json.dumps(result)
            )
        except:
            list_connection_loss.append(member[1])

    # connection이 loss 보정
    for connection_id in list_connection_loss:
        cursor.execute(f"update user_temp set connection_id=NULL where connection_id='{connection_id}'")

    connection.commit()
    connection.close()

    return {
        'statusCode': 200
    }
