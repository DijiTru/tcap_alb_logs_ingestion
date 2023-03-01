import logging

import psycopg2
from psycopg2 import sql
from pandas import DataFrame


class postgres_connection(object):
    """ create postgres connection"""

    def __init__(self, config):
        self.config = config
        self.connector = None
        self.cursor = None

    def __enter__(self):
        db_params = {}
        db_params['host'] = self.config.get('db', 'DB_HOST')
        db_params['user'] = self.config.get('db', 'DB_USER')
        db_params['database'] = self.config.get('db', 'DB_NAME')
        db_params['port'] = self.config.get('db', 'DB_PORT')
        db_params['password'] = self.config.get('db', 'DB_PASSWORD')
        logging.info(f"Logging into database with params {db_params}")
        self.connector = psycopg2.connect(**db_params)
        self.cursor = self.connector.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.cursor.close()
        self.connector.close()


def find_succesful_last_run_date(config):
    with postgres_connection(config) as db_conn:
        last_succesful_run_date_sql = 'select distinct run_date from tcap_analysis.tcap_alb_log_parsing_history where is_run_successful = TRUE order by run_date desc limit 1'
        db_conn.cursor.execute(last_succesful_run_date_sql)
        last_succesful_run_date = db_conn.cursor.fetchone()
    return last_succesful_run_date


def persist_object_data(df: DataFrame, config):
    with postgres_connection(config) as db_conn:
        for index, row in df.iterrows():
            query = sql.SQL("INSERT INTO tcap_analysis.tcap_alb_log_info ({}) VALUES ({})").format(
                sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
            )
            data = tuple(row)
            logging.info(f'executing query = {query} with data {data}')
            db_conn.cursor.execute(query, data)
