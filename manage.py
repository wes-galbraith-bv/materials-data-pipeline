import sys

from config import Config
from pipeline import Pipeline, MergePipeline
from csv_reader import Target, CSVReader
from db import DatabaseManager


if __name__ == '__main__':
    if sys.argv[1] == 'load':
        pipeline = MergePipeline() if set(sys.argv[2:]) & {'-m', '--merge'} else Pipeline()
        pipeline.run()
