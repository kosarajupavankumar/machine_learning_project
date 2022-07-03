from email import message
from tkinter.tix import COLUMN
from housing.logger import logging
from housing.exception import HousingException
from housing.entity.config_entity import DataValidationConfig
from housing.entity.artifact_entity import DataIngestionArtifact
import os, sys

class DataValidation:

    def __init__(self, data_validation_config : DataValidationConfig,
                data_ingestion_artifact : DataIngestionArtifact) -> None:
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise HousingException(e, sys) from e

    def is_train_test_file_exists(self):
        try:
            logging.info("Checking if training and test file is avaliable")
            is_train_file_exists = False
            is_test_file_exists = False
            if os.path.exists(self.data_ingestion_artifact.train_file_path):
                is_train_file_exists = True

            if os.path.exists(self.data_ingestion_artifact.test_file_path):
                is_test_file_exists = True

            is_available = is_train_file_exists and is_test_file_exists

            logging.info(f"Is train and test file exists?-> {is_available}")

            if not is_available:
                training_file = self.data_ingestion_artifact.train_file_path
                testing_file = self.data_ingestion_artifact.test_file_path
                message = f"Training file: {training_file} and testing file: {testing_file} are not present"
                raise HousingException(message)
            
            return is_available
        except Exception as e:
            raise HousingException(e, sys) from e

    
    def validate_dataset_schema(self) ->bool:
        try:
            validation_status = False

            #Assigment validate training and testing dataset using schema file 
            # 1. Number of column
            # 2. check the value of ocean proximity
            # acceptable values 
                    # - <1H OCEAN
                    # - INLAND
                    # - ISLAND
                    # - NEAR BAY
                    # - NEAR OCEAN
            # 3. check column names

            validation_status = True
            return validation_status

        except Exception as e:
            raise HousingException(e, sys) from e

    def initiate_data_validation(self):
        try:
            self.is_train_test_file_exists()
            self.validate_dataset_schema()

        except Exception as e:
            raise HousingException(e, sys) from e

    


