#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
import os
import pandas
import hashlib

from datetime import datetime
from templates import  get_bitcoin_template, get_ethereum_template

class QueryManager():
    def __init__(self):
        self.client = bigquery.Client()

    def do_query(self, currency:str,  query_type: str, limit: int, *args, **kwargs) -> pandas.DataFrame:
        sql = 'error'
        if currency in ['bitcoin', 'btc']:
            sql = get_bitcoin_template(query_type, limit, *args, **kwargs)
        elif currency in ['ethereum', 'eth']:
            sql = get_ethereum_template(query_type, limit, *args, **kwargs)
        label = "none"
        if label in kwargs.keys():
            label = kwargs['label']
        if sql == 'error':
            return pandas.DataFrame()
        query_id = hashlib.md5("{}{}".format(sql, label).encode())
        if not os.path.exists("./cache"):
            os.makedirs("./cache")
        df_dump = "./cache/{}".format(query_id.hexdigest())
        if os.path.isfile(df_dump):
            df = pandas.read_pickle(df_dump)
            return df
        else:
            df = self.client.query(sql).to_dataframe()
            df.to_pickle(df_dump)
            return df


if __name__ == '__main__':
    import sys
    import logging
    if len(sys.argv) < 2:
        logging.error("Google Application Credentials file must be provided as an argument")
        sys.exit(0)
    if not os.path.exists(sys.argv[1]):
        logging.error("Google Application Credentials file not found")
        sys.exit(0)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sys.argv[1]
    manager = QueryManager()
    date = datetime.now()
    label = datetime(*date.timetuple()[:3]).strftime('%s')
    df = manager.do_query('eth', 'transactions', 10, start_block=500000, label=label)
    print(df.head())
    print(df.dtypes)