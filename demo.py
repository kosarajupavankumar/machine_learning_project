from housing.logger import logging
from housing.pipeline.pipeline import Pipeline
from housing.exception import HousingException
def main():
    try:
        pipeline = Pipeline()
        pipeline.run_pipeline()
    except Exception as e:
        logging.error(e)
        raise HousingException(e, sys) from e


if __name__ == '__main__':
    main()

