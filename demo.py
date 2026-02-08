import sys
import os

from visa.logger import logging
from visa.exception import USVisaException
from visa.pipeline.training_pipeline import TrainingPipeline


pipeline = TrainingPipeline()
pipeline.run_pipeline()
