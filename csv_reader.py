import os
import pandas as pd
import re

class Target:
    def __init__(self, directory, filename_regex, table_name, aliases):
        self.directory = directory
        self.filename_regex = filename_regex
        self.table_name = table_name
        self.aliases = aliases

    def filter(self, f):
        return bool(re.match(self.filename_regex, f.name))


class CSVReader:
    def __init__(self, root, targets=None):
        self.root = root
        if targets:
            self.targets = list(*targets)

    def read(self):
        for t in self.targets:
            path = os.path.join(os.path.abspath(self.root), t.directory)
            files = (f for f in os.scandir(path) if f.is_file() and t.filter(f))
            most_recent = max(files, key=lambda f: f.stat().st_ctime)
            path_to_csv = most_recent.path
            usecols = lambda c: c in t.aliases
            df = pd.read_csv(path_to_csv, usecols=usecols)
            df.columns = [t.aliases[c] for c in df.columns]
            df.name = t.table_name
            yield df
