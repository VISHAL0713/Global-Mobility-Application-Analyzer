import json
import sys
import pandas as pd
from pandas import DataFrame

from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection

from visa.exception import USVisaException
from visa.logger import logging
from visa.utils.main_utils import read_yaml_file, write_yaml_file
from visa.entity.artifact_entity import DataValidationArtifact, DataIngestionArtifact
from visa.entity.config_entity import DataValidationConfig
from visa.constants import SCHEMA_FILE_PATH


class DataValidation:
    def __init__(self, data_validation_config: DataValidationConfig, data_ingestion_artifact: DataIngestionArtifact):
        """This is the constructor of the DataValidation class which initializes the data validation configuration, data ingestion artifact and schema config.
        Input           :  data_validation_config: DataValidationConfig, data_ingestion_artifact: DataIngestionArtifact
        Output          :  None 
        on Failure      :  raise exception
        """
        try:
            logging.info(f"{'>>'*20} Data Validation {'<<'*20}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def validate_number_of_columns(self, dataframe: DataFrame) -> bool:
        """This function validates the number of columns in the given dataframe with the expected number of columns in the schema config.
        Input           :  dataframe: DataFrame
        Output          :  bool
        on Failure      :  raise exception
        """
        try:
            status = len(dataframe.columns) == len(self._schema_config["columns"])
            logging.info(f"Number of columns validation status: {status}")
            return status
        except Exception as e:
            raise USVisaException(e, sys) from e
    
    def is_column_exist(self,df:DataFrame) -> bool:
        """This function validates the presence of all the expected columns in the given dataframe as per the schema config.
        Input           :  dataframe: DataFrame
        Output          :  bool
        on Failure      :  raise exception
        """
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)
            
            if len(missing_numerical_columns) > 0:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")
            
            for column in self._schema_config["categorical_columns"]:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)
            
            if len(missing_categorical_columns) > 0:
                logging.info(f"Missing categorical columns: {missing_categorical_columns}")
                
            return False if len(missing_numerical_columns) > 0 or len(missing_categorical_columns) > 0 else True
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    @staticmethod
    def read_data(file_path: str) -> DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def check_data_type(self, dataframe: DataFrame) -> bool:
        """This function validates the data types of the columns in the given dataframe with the expected data types in the schema config.
        Input           :  dataframe: DataFrame
        Output          :  bool
        on Failure      :  raise exception
        """
        try:
            status = True
            for column in self._schema_config["numerical_columns"]:
                if column in dataframe.columns and not pd.api.types.is_numeric_dtype(dataframe[column]):
                    logging.info(f"Column: {column} is expected to be numerical but found {dataframe[column].dtype}")
                    status = False
            
            for column in self._schema_config["categorical_columns"]:
                if column in dataframe.columns and not pd.api.types.is_object_dtype(dataframe[column]):
                    logging.info(f"Column: {column} is expected to be categorical but found {dataframe[column].dtype}")
                    status = False
            
            logging.info(f"Data type validation status: {status}")
            return status
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def detect_dataset_drift(self, base_df: DataFrame, current_df: DataFrame) -> bool:
        """This function detects the dataset drift between the base dataframe and the current dataframe using the evidently library and returns the data drift report.
        Input           :  base_df: DataFrame, current_df: DataFrame
        Output          :  Profile
        on Failure      :  raise exception
        """
        try:
            data_drift_profile = Profile(sections=[DataDriftProfileSection()])
            data_drift_profile.calculate(base_df, current_df)
            
            report = data_drift_profile.json()
            json_report = json.loads(report)
            
            write_yaml_file(file_path=self.data_validation_config.drift_report_file_path, content=json_report)
            
            n_features = json_report["data_drift"]["data"]["metrics"]["n_features"]
            n_drifted_features = json_report["data_drift"]["data"]["metrics"]["n_drifted_features"]
            
            logging.info(f"{n_drifted_features} out of {n_features} features are drifted.")
            logging.info(f"{n_drifted_features/n_features*100} % of features are drifted.")
            drift_status = json_report["data_drift"]["data"]["metrics"]["dataset_drift"]
            return drift_status
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Method Name :   initiate_data_validation
        Description :   This method initiates the data validation component for the pipeline
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """

        try:
            validation_error_msg = ""
            logging.info("Starting data validation")
            train_df, test_df = (DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                                 DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path))

            status = self.validate_number_of_columns(dataframe=train_df)
            logging.info(f"All required columns present in training dataframe: {status}")
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            status = self.validate_number_of_columns(dataframe=test_df)

            logging.info(f"All required columns present in testing dataframe: {status}")
            if not status:
                validation_error_msg += f"Columns are missing in test dataframe."

            status = self.is_column_exist(df=train_df)

            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            status = self.is_column_exist(df=test_df)

            if not status:
                validation_error_msg += f"columns are missing in test dataframe."

            validation_status = len(validation_error_msg) == 0

            if validation_status:
                drift_status = self.detect_dataset_drift(train_df, test_df)
                if drift_status:
                    logging.info(f"Drift detected.")
                    validation_error_msg = "Drift detected"
                else:
                    validation_error_msg = "Drift not detected"
            else:
                logging.info(f"Validation_error: {validation_error_msg}")
                

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message=validation_error_msg,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    
