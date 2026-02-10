import sys
from visa.exception import USVisaException
from visa.logger import logging 
from visa.components.data_ingestion import DataIngestion
from visa.components.data_validation import DataValidation
from visa.entity.config_entity import DataIngestionConfig, DataValidationConfig
from visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact


class TrainingPipeline:
    def __init__(self):
        try:
            self.data_ingestion_config = DataIngestionConfig()
            self.data_validation_config = DataValidationConfig()
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def start_data_ingestion(self) -> DataIngestionArtifact:
        """
        This function starts the data ingestion of the training pipeline and returns the 
        artifact of data ingestion containing the file paths of the ingested training and testing data.

        Output           :  DataIngestionArtifact containing the file paths of the ingested training and testing data
        on Failure       :  raise exception
        """
        try:
            logging.info(f"Data Ingestion of the TrainingPipeline is started")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info(f"Data Ingestion of the TrainingPipeline is completed")
            return data_ingestion_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) -> DataValidationArtifact:
        """
        This function starts the data validation of the training pipeline and returns the 
        artifact of data validation containing the validation status, message and file path of the drift report.

        Input           :  data_ingestion_artifact: DataIngestionArtifact containing the file paths of the ingested training and testing data
        Output          :  DataValidationArtifact containing the validation status, message and file path of the drift report
        on Failure      :  raise exception
        """
        try:
            logging.info(f"Data Validation of the TrainingPipeline is started")
            data_validation = DataValidation(data_validation_config=self.data_validation_config, data_ingestion_artifact=data_ingestion_artifact)
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info(f"Data Validation of the TrainingPipeline is completed")
            return data_validation_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def run_pipeline(self):
        """
        This function runs the entire training pipeline.
        Output           :  None
        on Failure       :  raise exception
        """
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
        except Exception as e:
            raise USVisaException(e, sys) from e