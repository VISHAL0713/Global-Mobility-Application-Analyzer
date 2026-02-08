import sys
from visa.exception import USVisaException
from visa.logger import logging 
from visa.components.data_ingestion import DataIngestion
from visa.entity.config_entity import DataIngestionConfig
from visa.entity.artifact_entity import DataIngestionArtifact


class TrainingPipeline:
    def __init__(self,data_ingestion_config: DataIngestionConfig = DataIngestionConfig()):
        try:
            self.data_ingestion_config = data_ingestion_config
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
        
        
    def run_pipeline(self):
        """
        This function runs the entire training pipeline.
        Output           :  None
        on Failure       :  raise exception
        """
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise USVisaException(e, sys) from e