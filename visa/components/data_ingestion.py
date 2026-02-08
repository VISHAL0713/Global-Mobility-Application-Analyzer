import os
import sys
from pandas import DataFrame
from sklearn.model_selection import train_test_split

from visa.entity.config_entity import DataIngestionConfig
from visa.entity.artifact_entity import DataIngestionArtifact
from visa.exception import USVisaException
from visa.logger import logging
from visa.data_access.visa_data import VisaData


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig = DataIngestionConfig()):
        """
            This class is responsible for data ingestion from MongoDB
            Output           :  DataIngestionArtifact containing the file paths of the ingested training and testing data
            on Failure       :  raise exception
        """
        try:
            logging.info(f"{'>>'*20} Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise USVisaException(e, sys) from e
        
        
    def export_data_into_feature_store(self) -> DataFrame:
        """
            This function exports the data from MongoDB collection to a feature store as a pandas dataframe.
            Output           :  DataFrame containing the data from MongoDB collection
            on Failure       :  raise exception
        """
        try:
            logging.info(f"Exporting data from collection: {self.data_ingestion_config.collection_name} to feature store.")
            visa_data = VisaData()
            visa_dataframe = visa_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            logging.info(f"Shape of the exported dataframe: {visa_dataframe.shape}")
            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir, exist_ok=True)
            visa_dataframe.to_csv(self.data_ingestion_config.feature_store_file_path, index=False)
            logging.info(f"Data exported to feature store at: {self.data_ingestion_config.feature_store_file_path}")
            return visa_dataframe
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        """
            This function splits the dataframe into training and testing data and saves them in the ingested directory.
            Output           :  DataIngestionArtifact containing the file paths of the ingested training and testing data
            on Failure       :  raise exception
        """
        try:
            logging.info(f"Splitting data into train and test sets with test size: {self.data_ingestion_config.train_test_split_ratio}")
            train_set, test_set = train_test_split(dataframe, test_size=self.data_ingestion_config.train_test_split_ratio, random_state=42)
            logging.info(f"Train set shape: {train_set.shape}, Test set shape: {test_set.shape}")
            
            ingested_dir = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(ingested_dir, exist_ok=True)
            
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False,header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False,header=True)
            
            logging.info(f"Training data saved at: {self.data_ingestion_config.training_file_path}")
            logging.info(f"Testing data saved at: {self.data_ingestion_config.testing_file_path}")
            
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
            This function initiates the data ingestion process by calling the functions to export data into feature store and split data into train and test sets.
            Output           :  DataIngestionArtifact containing the file paths of the ingested training and testing data
            on Failure       :  raise exception
        """
        try:
            dataframe = self.export_data_into_feature_store()
            logging.info("Exported data from MongoDB collection to feature store successfully.")
            
            self.split_data_as_train_test(dataframe=dataframe)
            logging.info("Split data into train and test sets and saved them successfully.")
            
            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            logging.info(f"Data Ingestion Artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e
        
        
    


