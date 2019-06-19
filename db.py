from datetime import datetime
from logger import logger
import urllib

import pandas as pd
import sqlalchemy as sa

from config import Config

class DatabaseManager:
    def __init__(self, driver, username, password, server, database, schema):
        logger.info('creating database manager')
        self.driver = driver
        self.username = username
        self.password = password
        self.server = server
        self.database = database
        self.schema = schema

        self.metadata = sa.MetaData()

        conn_str = (f'Driver={driver};UID={username};'
            f'PWD={password};SERVER={server};'
            f'DATABASE={database};MARS_Connection=Yes')

        quoted = urllib.parse.quote_plus(conn_str)
        self.engine = sa.create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}',
                                       fast_executemany=True)

    @classmethod
    def from_object(cls, config):
        return cls(config.driver, config.username, config.password, config.server, config.database, config.schema)


    def insert_df(self, df):
        logger.info(f'inserting {len(df)} records from {df.name}')
        con = self.engine.connect()
        dtype = Config.dtypes[df.name]
        df.to_sql(df.name, schema=self.schema, con=con, if_exists='replace', index=False, chunksize=25000, dtype=dtype)
        logger.info(f'done inserting')


db = DatabaseManager.from_object(Config)
