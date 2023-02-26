import argparse
import configparser
import logging
from datetime import datetime, timedelta
import pathlib

import boto3

from alb_parser import read_from_s3
from tcap_alb_logs_ingestion.db_ops import find_succesful_last_run_date
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


def find_files_for_prefix(prefix):
    pass


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
