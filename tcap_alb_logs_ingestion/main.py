import argparse
import configparser
import logging, re
from datetime import datetime, timedelta
import pathlib
import io, gzip
import pandas as pd

import boto3

from alb_parser import read_from_s3
from tcap_alb_logs_ingestion.db_ops import find_succesful_last_run_date, persist_object_data
from utils import read_json, write_json

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')
dt_fmt = '%Y-%m-%d'


def find_all_prefixes_tosearch_for(search_start_date, base_s3_path):
    prefixes_to_search = []
    current_date = datetime.today()
    date_generated = [search_start_date + timedelta(days=x) for x in
                      range(0, (current_date - search_start_date).days + 1)]
    base_prefix = base_s3_path.replace('s3://', '')
    for date in date_generated:
        m = '{:02d}'.format(date.month)
        d = '{:02d}'.format(date.day)
        path = f'{base_prefix}/{date.year}/{m}/{d}/'
        prefixes_to_search.append(path)
    return prefixes_to_search


def find_starting_point_to_parse(last_good_run_date, start_date):
    logging.info(f"last good run date  = {last_good_run_date}")
    if last_good_run_date is None:
        logging.info("Possibly first run, starting from the base s3 path")
        return datetime.strptime(start_date, dt_fmt)
    else:
        return last_good_run_date


def get_gz_content(bucket_name, key):
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket_name, Key=key)
    compressed_stream = io.BytesIO(s3_object['Body'].read())
    gzip_file = gzip.GzipFile(fileobj=compressed_stream)
    return gzip_file.readlines()


def parselines(line):
    entries = {}
    fields = [
        "type",
        "timestamp",
        "alb",
        "client_ip",
        "client_port",
        "backend_ip",
        "backend_port",
        "request_processing_time",
        "backend_processing_time",
        "response_processing_time",
        "alb_status_code",
        "backend_status_code",
        "received_bytes",
        "sent_bytes",
        "request_verb",
        "request_url",
        "request_proto",
        "user_agent",
        "ssl_cipher",
        "ssl_protocol",
        "target_group_arn",
        "trace_id",
        "domain_name",
        "chosen_cert_arn",
        "matched_rule_priority",
        "request_creation_time",
        "actions_executed",
        "redirect_url",
        "new_field",
    ]
    # Note: for Python 2.7 compatibility, use ur"" to prefix the regex and u"" to prefix the test string and substitution.
    # REFERENCE: https://docs.aws.amazon.com/athena/latest/ug/application-load-balancer-logs.html#create-alb-table
    regex = r"([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" ($|\"[^ ]*\")(.*)"
    matches = re.search(regex, line)
    if matches:
        for i, field in enumerate(fields):
            end = ", " if i < len(fields) - 1 else "\n"
            entries[field] = matches.group(i + 1)
    logging.debug(entries)
    return entries


def parse_and_insert(alb_logs_bucket, key):
    lines = get_gz_content(alb_logs_bucket, key)
    full_file_path = f's3://{alb_logs_bucket}/{key}'
    logging.info(f"Found {len(lines)} for file {full_file_path}")


def main(config):
    # Set up S3 client
    # S3 access key unavailable
    starting_point = find_starting_point_to_parse(find_succesful_last_run_date(config),
                                                  config.get('s3', 'start_date'))
    prefixes_to_search = find_all_prefixes_tosearch_for(starting_point, config.get('s3', 'BASE_S3_PATH'))
    path = pathlib.PurePath(config.get('s3', 'BASE_S3_PATH'))
    alb_logs_bucket = path.parts[1]
    print(f"Last good run date {starting_point}, from the s3 alb logs bucket  {alb_logs_bucket}")

    # create a session with IAM roles
    s3 = boto3.resource('s3')
    for prefix in prefixes_to_search:
        prefix = prefix.replace(alb_logs_bucket + '/', '')
        logging.info(f"Searching for prefix - {prefix} in {alb_logs_bucket}")
        objects = s3.Bucket(alb_logs_bucket).objects.filter(Prefix=prefix)
        all_objects = list(objects.all())
        logging.info(f'found {len(all_objects)} files for {prefix} in bucket {alb_logs_bucket}')
        for object in all_objects:
            logging.info(f"Found object {object.key} with prefix  {prefix}")
            lines = get_gz_content(alb_logs_bucket, object.key)
            entries = []
            for line in lines:
                logging.info(f"Line post parsing from gz {line} from file {object.key}")
                entries.append(parselines(str(line)))
            df = pd.DataFrame(entries)
            df['file_name'] = object.key
            persist_object_data(df, config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--env_name', dest='env_name', help="Name of the env to run with")
    args = parser.parse_args()
    env_name = args.env_name
    config = configparser.ConfigParser()
    match env_name:
        case "dev":
            logging.info("Loading properties for dev ")
            config.read('resources/dev.properties')
        case "qa":
            logging.info("Loading properties for qa")
            config.read('resources/qa.properties')
        case "preprod":
            logging.info("Loading properties for preprod")
            config.read('resources/preprod.properties')
        case "prod":
            logging.info("Loading properties for prod")
            config.read("resources/prod.properties")
        case _:
            logging.info("No environment specified , so loading properties for local/satish")
            config.read("resources/satish.properties")
    main(config)
