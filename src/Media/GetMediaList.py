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
            f"select id, user_id, `key`, send_at from media_temp where family_id={family_id} order by send_at DESC, `order` LIMIT {count};")
    else:
        cursor.execute(f"select `order`, send_at from media_temp where id = {last_id};")
        order, send_at = cursor.fetchall()[0]

        cursor.execute(
            f"""select id, user_id, `key`, send_at from media_temp 
                where (family_id={family_id} 
                and ((send_at='{send_at}' and `order`>{order}) or send_at < '{send_at}'))
                order by send_at DESC, `order` 
                LIMIT {count};""")

    response = cursor.fetchall()
    connection.close()

    # query 결과를 list of objects로 만들기
    list_media = []
    for media in response:
        media_object = {
            'user_id': media[1],
            'key': media[2],
            'send_at': datetime.timestamp(media[3])
        }
        list_media.append(media_object)

    result = {"album": list_media}

    if len(response) != 0:
        result["last_id"] = response[len(response) - 1][0]
    else:
        # 더이상 없을 때
        result["last_id"] = -1

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
