import os
import pymysql


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
    connection_id = event['requestContext']['connectionId']
    user_id = int(event['queryStringParameters']['u'])

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    cursor.execute(f"update user_temp set connection_id='{connection_id}' where id={user_id}")

    connection.commit()
    connection.close()

    return {
        'statusCode': 200
    }
