import logging
import sys


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


def get_logger():

    logger = logging.getLogger()

    file_handler = logging.FileHandler('logs/error.log')
    file_handler.setLevel(logging.ERROR)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    return logger


class Loggable:
    def __repr__(self):
        constructor = type(self).__name__
        args = ','.join(f'{k}={v}' for (k, v) in vars(self).items())
        return f"{constructor}({args})"
