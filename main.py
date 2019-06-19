import logging

from pipeline import MergePipeline


def main():
    logging.basicConfig(filename='pipeline.log', level=logging.INFO,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p'))
    logging.info('creating pipeline')
    pipeline = MergePipeline()
    try:
        pipeline.run()
    except Exception as e:
        logging.error(str(e))


if __name__ == '__main__':
    main()
