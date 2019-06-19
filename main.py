import os

from logger import logger
from pipeline import MergePipeline


def main():
    logger.info('creating pipeline')
    pipeline = MergePipeline()
    try:
        pipeline.run()
    except Exception as e:
        logger.error(str(e), exc_info=True)


if __name__ == '__main__':

    main()
