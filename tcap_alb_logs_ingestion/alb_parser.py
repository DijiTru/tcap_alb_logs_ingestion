import os
import boto3
import gzip
import io
import pandas as pd
from psycopg2 import connect, sql

s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('AWS_ACCESS_KEY'),
                         aws_secret_access_key=os.environ.get('AWS_SECRET_KEY'))


def get_gz_content(key):
    s3_object = s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=key)
    compressed_stream = io.BytesIO(s3_object['Body'].read())
    gzip_file = gzip.GzipFile(fileobj=compressed_stream)

    return gzip_file.readlines()


def parseS3Logs(logs):
    parsed_entries = {
        'type': logs.split(' ')[0],
        'timestamp': logs.split(' ')[1],
        'elb': logs.split(' ')[2],
        'client_ip': logs.split(' ')[3].split(':')[0],
        'client_port': logs.split(' ')[3].split(':')[1],
        'target_ip': logs.split(' ')[4].split(':')[0],
        'target_port': logs.split(' ')[4].split(':')[1],
        'request_processing_time': logs.split(' ')[5],
        'target_processing_time': logs.split(' ')[6],
        'response_processing_time': logs.split(' ')[7],
        'elb_status_code': logs.split(' ')[8],
        'target_status_code': logs.split(' ')[9],
        'received_bytes': logs.split(' ')[10],
        'sent_bytes': logs.split(' ')[11],
        'request_verb': logs.split(' ')[12],
        'request_url': logs.split(' ')[13],
        'request_proto': logs.split(' ')[14],
        'user_agent': ' '.join(logs.split(' ')[15:-1]),
        'ssl_cipher': logs.split(' ')[-1].split(':')[0],
        'ssl_protocol': logs.split(' ')[-1].split(':')[1].strip()
    }

    return parsed_entries


def read_from_s3(year, month, day):
    dir_path = os.environ['S3_DIR_PATH'] + str(year) + '/' + str(
        month).rjust(2, '0') + '/' + str(day).rjust(2, '0') + '/' + os.environ['S3_PREFIX']
    response = s3_client.list_objects_v2(
        Bucket=os.environ['S3_BUCKET'],
        Prefix=dir_path,
        MaxKeys=1000
    )
    # print(response)
    entries = []
    for content in response.get('Contents', []):
        lines = get_gz_content(content['Key'])

        for line in lines:
            entries.append(parseS3Logs(str(line)))

    df = pd.DataFrame(entries)

    conn = connect(
        dbname=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT')
    )

    with conn:
        with conn.cursor() as curs:
            for index, row in df.iterrows():
                query = sql.SQL("INSERT INTO alb_logs ({}) VALUES ({})").format(
                    sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                )
                data = tuple(row)
                curs.execute(query, data)

    print(f'{len(entries)} entries have been inserted into database.')
