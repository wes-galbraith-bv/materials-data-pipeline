import sys

from config import Config
from csv_reader import Target, CSVReader
from db import DatabaseManager

def load(Config):
    db = DatabaseManager(Config.driver, Config.username, Config.password,
                         Config.server, Config.database, Config.schema)
    csv_reader = CSVReader(Config.directory)
    csv_reader.targets = [Target(*params) for params in Config.targets]
    for df in csv_reader.read():
        con = db.engine.connect()
        db.insert_df(df)

if __name__ == '__main__':
    if sys.argv[1] == 'load':
        load(Config)
