import sys
from visa.exception import USVisaException
from visa.logger import logging
import os
from visa.constants import MONGODB_URL_KEY, DATABASE_NAME
import pymongo
import certifi

ca = certifi.where()

class MongoDBClient:
    """
    This class helps to create the MongoDB client and connect with the database.
    Output           :  connection to MongoDB database
    on Failure       :  raise exception
    """
    
    client = None
    
    def __init__(self, database_name: str = DATABASE_NAME):
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception(f"Environment variable: {MONGODB_URL_KEY} is not set.", sys)
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info(f"MongoDB client connected successfully to the database: {database_name}")
        except Exception as e:
            raise USVisaException(e, sys) from e