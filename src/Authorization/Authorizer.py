import os
import jwt
import pymysql


UNAUTHORIZED_RESPONSE = {'isAuthorized': False}
AUTHORIZED_RESPONSE = {'isAuthorized': True}


def get_jwt_secrets():
    secrets = {
        'key': os.environ.get('key'),
        'algorithm': os.environ.get('algorithm'),
        'db': os.environ.get('db'),
        'host': os.environ.get('host'),
        'password': os.environ.get('password'),
        'port': int(os.environ.get('port')),
        'username': os.environ.get('username'),
    }

    return secrets


def valid_check(payload, secrets):
    connection = pymysql.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        passwd=secrets['password'],
        db=secrets['db']
    )
    cursor = connection.cursor()

    cursor.execute(f"select count(*) from user_temp where id = {payload['memberId']}")
    count = cursor.fetchall()[0][0]
    connection.close()

    if count != 1:
        return False

    return True


def lambda_handler(event, context):
    secrets = get_jwt_secrets()
    token_data = event["identitySource"][0]
    token_type, token = token_data.split(' ')

    try:
        payload = jwt.decode(token, key=secrets['key'], algorithms=secrets['algorithm'])
    except:
        return UNAUTHORIZED_RESPONSE

    is_valid = valid_check(payload, secrets)

    if not is_valid:
        return UNAUTHORIZED_RESPONSE

    return AUTHORIZED_RESPONSE
