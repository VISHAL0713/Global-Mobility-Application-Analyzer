from visa.constants import DATABASE_NAME
from visa.exception import USVisaException
from visa.logger import logging
import pandas as pd
import sys
from typing import Optional
import numpy as np
from visa.configuration.mongo_db_connection import MongoDBClient



class VisaData:
    """
    This class helps to export entire mongo db record as pandas dataframe.
    """
    
    def __init__(self):
            try:
                self.mongo_client = MongoDBClient(database_name = DATABASE_NAME)
            except Exception as e:
                raise USVisaException(e, sys) from e
            
    def export_collection_as_dataframe(self, collection_name: str,database_name:Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        This function returns the entire record of the collection as a pandas dataframe.
        Output           :  DataFrame containing the entire record of the collection
        on Failure       :  raise exception
        """
        try:
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client.client[database_name][collection_name]
                
            df = pd.DataFrame(list(collection.find()))
            logging.info(f"Data from collection: {collection_name} has been exported as dataframe successfully.")
            if "_id" in df.columns:
                df.drop("_id", axis=1, inplace=True)
            df.replace({"na": np.nan}, inplace=True)
            return df
        except Exception as e:
            raise USVisaException(e, sys) from e
            