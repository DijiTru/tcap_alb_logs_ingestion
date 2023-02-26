import argparse
import configparser
import logging
from datetime import datetime, timedelta

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
    for date in date_generated:
        m = '{:02d}'.format(date.month)
        d = '{:02d}'.format(date.day)
        path = f'{base_s3_path}/{date.year}/{m}/{d}'
        logging.info(f"Path to append  = {path}")
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
    print(f"Last good run date {starting_point}")

    # create a session with IAM roles
    session = boto3.Session()

    # create an S3 client using the session
    s3_client = session.client('s3')

    # Read last sync date from JSON file
    last_sync_obj = read_json()
    last_sync_string = "{}-{}-{}".format(last_sync_obj['ls_year'],
                                         last_sync_obj['ls_month'], last_sync_obj['ls_day'])
    last_sync = datetime.strptime(last_sync_string, '%Y-%m-%d')
    # Calculate next day to sync and current day for comparison
    next_day = last_sync + timedelta(days=1)
    sync_upto = datetime.utcnow() - timedelta(days=1)
    # Check if sync process is already up to date
    if next_day > sync_upto:
        print('WARN: Sync process is already up to date')
        return
    # Loop through each day from last sync day to current day and sync data
    while next_day <= sync_upto:
        print(next_day)
        read_from_s3(next_day.year, next_day.month, next_day.day)
        next_day = next_day + timedelta(days=1)
    # wpdate last sync date in JSON file
    last_sync = next_day - timedelta(days=1)
    write_json(last_sync.year, last_sync.month, last_sync.day)


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
