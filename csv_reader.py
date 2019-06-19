from logger import logger
import os
import re

import pandas as pd


class Target:
    def __init__(self, directory, filename_regex, table_name, aliases):
        logger.info('creating target for csv reader')
        self.directory = directory
        self.filename_regex = filename_regex
        self.table_name = table_name
        self.aliases = aliases

    def filter(self, f):
        logger.info(f'applying filter {self.filename_regex} to filenames')
        return bool(re.match(self.filename_regex, f.name))

    def __str__(self):
        return self.directory


class CSVReader:
    def __init__(self, root, targets=None):
        logger.info(f'creating CSVReader for directory {root}')
        self.root = root
        if targets:
            logger.info(f'target directories: {";".join(t for t in targets)}')
            self.targets = list(*targets)

    def read(self):
        for t in self.targets:
            logger.info(f'reading from subdirectory {t}')
            path = os.path.join(os.path.abspath(self.root), t.directory)
            files = (f for f in os.scandir(path) if f.is_file() and t.filter(f))
            most_recent = max(files, key=lambda f: f.stat().st_ctime)
            path_to_csv = most_recent.path
            logger.info(f'Most recent matching file: {path_to_csv}')
            usecols = lambda c: c in t.aliases
            df = pd.read_csv(path_to_csv, usecols=usecols)
            df.columns = [t.aliases[c] for c in df.columns]
            df.name = t.table_name
            yield df
