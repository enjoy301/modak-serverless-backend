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

    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    cursor.execute(f'DELETE from message_temp;')

    connection.commit()
    connection.close()

    return {
        'statusCode': 200
    }
