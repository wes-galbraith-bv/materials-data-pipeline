import urllib

import sqlalchemy as sa
import pandas as pd

from config import Config

class DatabaseManager:
    def __init__(self, driver, username, password, server, database, schema):
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
        print(f'inserting records from {df.name}')
        con = self.engine.connect()
        df.to_sql(f'{self.schema}.{df.name}', con=con, if_exists='replace', index=False, chunksize=25000)


db = DatabaseManager.from_object(Config)
