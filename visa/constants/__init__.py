
"""
This module contains all the constant values used in the visa application analysis project. These constants include 
database names, collection names, file names, target column name, and AWS credentials environment variable keys. 
By centralizing these values in a single module, we can easily manage and update them as needed throughout the project.
"""



import os
from datetime import date

### Database Constants
DATABASE_NAME = "VISA_APPLICATION_DATA"
COLLECTION_NAME = "visa_data"
MONGODB_URL_KEY = "MONGODB_URL"


PIPELINE_NAME: str = "visa"
ARTIFACTS_DIR: str = "artifacts"

MODEL_FILE_NAME = "model.pkl"
TARGET_COLUMN: str = "case_status"
CURRENT_YEAR = date.today().year
PREPROCESSING_OBJECT_FILE_NAME = "preprocessor.pkl"

FILE_NAME: str = "visa_data.csv"
TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"
SCHEMA_FILE_PATH = os.path.join("config", "schema.yaml")


### AWS Credentials Constants
AWS_ACCESS_KEY_ID_ENV_KEY = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY_ENV_KEY = "AWS_SECRET_ACCESS_KEY"
REGION_NAME = "us-east-1"

### Data Ingestion Constants
DATA_INGESTION_COLLECTION_NAME: str = "visa_data"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.2


### Data Validation Constant
DATA_VALIDATION_DIR_NAME: str = "data_validation"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME: str = "report.yaml"