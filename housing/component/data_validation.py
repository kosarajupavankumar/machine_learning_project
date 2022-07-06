import yaml
from housing.logger import logging
from housing.exception import HousingException
from housing.entity.config_entity import DataValidationConfig
from housing.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
import os, sys
import json
import pandas as pd

from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection
from evidently.dashboard import Dashboard
from evidently.dashboard.tabs import DataDriftTab
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class DataValidation:

    def __init__(self, data_validation_config : DataValidationConfig,
                data_ingestion_artifact : DataIngestionArtifact) -> None:
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise HousingException(e, sys) from e

    def get_train_and_test_df(self):
        try:
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            return train_df, test_df
        except Exception as e:
            raise HousingException(e, sys) from e


    def is_train_test_file_exists(self):
        try:
            logging.info("Checking if training and test file is avaliable")
            is_train_file_exist = False
            is_test_file_exist = False
            
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            is_train_file_exist = os.path.exists(train_file_path)
            is_test_file_exist = os.path.exists(test_file_path)

            is_available = is_train_file_exist and is_test_file_exist

            logging.info(f"Is train and test file exists?-> {is_available}")

            if not is_available:
                training_file = self.data_ingestion_artifact.train_file_path
                testing_file = self.data_ingestion_artifact.test_file_path
                message = f"Training file: {training_file} and testing file: {testing_file} are not present"
                raise Exception(message)
            
            return is_available

        except Exception as e:
            raise HousingException(e, sys) from e

    
    def validate_dataset_schema(self) ->bool:
        try:
            validation_status = False
            schema_file_path = self.data_validation_config.schema_file_path
            
            if os.path.exists(schema_file_path):
                with open(schema_file_path) as file_path:
                    schema_dict = yaml.safe_load(file_path)

                train_df, test_df = self.get_train_and_test_df()

                #Check number of columns for training and testing dataset with schema file
                if len(train_df.columns) == len(schema_dict['columns']) and len(test_df.columns) == len(schema_dict['columns']):
                    logging.info("Number of columns are same in training and testing dataset")
                    validation_status = True
                else:
                    raise HousingException("Number of columns are not same in training and testing dataset")

                #Check column names for training and testing dataset with schema file using isin functions
                if train_df.columns.isin(schema_dict['columns']).all() and test_df.columns.isin(schema_dict['columns']).all():
                    logging.info("Column names are same in training and testing dataset")
                    validation_status = True
                else:
                    raise HousingException("Column names are not same in training and testing dataset")

                #Check ocean proximity value for training and testing dataset with schema file
                if (list(train_df['ocean_proximity'].unique()).sort() == schema_dict['domain_value']['ocean_proximity'].sort()) and list(test_df['ocean_proximity'].unique()).sort() ==schema_dict['domain_value']['ocean_proximity'].sort():
                    logging.info("Ocean proximity values are same in training and testing dataset")
                    validation_status = True
                else:
                    raise HousingException("Ocean proximity values are not same in training and testing dataset")

            return validation_status
        except Exception as e:
            raise HousingException(e, sys) from e

    def is_data_drift_found(self)->bool:
        try:
            report = self.get_and_save_data_drift_report()
            self.save_data_drift_report_page()
            return True
        except Exception as e:
            raise HousingException(e, sys) from e

    def get_and_save_data_drift_report(self):
        try:
            profile = Profile(sections=[DataDriftProfileSection()])
            train_df, test_df = self.get_train_and_test_df()
            profile.calculate(train_df, test_df)
            profile.json()
            report = json.loads(profile.json())

            report_file_path = self.data_validation_config.report_file_path

            report_dir = os.path.dirname(report_file_path)
            os.makedirs(report_dir, exist_ok=True)

            with open(report_file_path,"w") as report_file:
                json.dump(report, report_file, indent=6)

            return report

        except Exception as e:
            raise HousingException(e, sys) from e

    def save_data_drift_report_page(self):
        try:
            dashboard = Dashboard(tabs=[DataDriftTab()])
            train_df, test_df = self.get_train_and_test_df()
            dashboard.calculate(train_df, test_df)
            report_page_file_path = self.data_validation_config.report_page_file_path

            report_page_dir = os.path.dirname(report_page_file_path)
            os.makedirs(report_page_dir, exist_ok=True)
            dashboard.save(report_page_file_path)

        except Exception as e:
            raise HousingException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            self.is_train_test_file_exists()
            self.validate_dataset_schema()
            self.is_data_drift_found()

            data_validation_artifact = DataValidationArtifact(schema_file_path= self.data_validation_config.schema_file_path,
                                                            report_file_path= self.data_validation_config.report_file_path,
                                                            report_page_file_path= self.data_validation_config.report_page_file_path,
                                                            is_validated= True,
                                                            message= "Data validation performed successfully")

            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:  
            raise HousingException(e, sys) from e

    


