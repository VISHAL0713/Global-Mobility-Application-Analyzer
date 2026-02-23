import sys
import os
from typing import Tuple
import pandas as pd
import numpy as np
from pandas import DataFrame

from sklearn.pipeline import Pipeline
from sklearn.metrics import f1_score, precision_score, recall_score,accuracy_score
from neuro_mf import ModelFactory


from visa.logger import logging
from visa.exception import USVisaException
from visa.entity.config_entity import ModelTrainerConfig
from visa.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact, ClassificationMetricArtifact
from visa.utils.main_utils import load_numpy_array_data, save_object, load_object,read_yaml_file
from visa.entity.estimator import VisaModel



class ModelTrainer:
    def __init__(self, model_trainer_config: ModelTrainerConfig, data_transformation_artifact: DataTransformationArtifact):
        """_summary_

        Args:
            model_trainer_config (ModelTrainerConfig): _description_
            data_transformation_artifact (DataTransformationArtifact): _description_

        Raises:
            USVisaException: _description_
        """
        try:
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e
        
    def get_model_object_and_report(self,train: np.array, test: np.array) -> Tuple[object,object]:
        try:
            logging.info("Using NeuroMF to obtain the best model and report for the given training and testing data")
            model_factory = ModelFactory(model_config_path = self.model_trainer_config.model_config_file_path)
            
            x_train,y_train,x_test,y_test = train[:,:-1],train[:,-1],test[:,:-1],test[:,-1]
            best_model_detail = model_factory.get_best_model(x_train,y_train,
                                                             base_accuracy=self.model_trainer_config.expected_accuracy)
            model_obj = best_model_detail.best_model
            y_pred = model_obj.predict(x_test)
            
            accuracy = accuracy_score(y_test,y_pred)
            precision = precision_score(y_test,y_pred)
            recall = recall_score(y_test,y_pred)
            f1 = f1_score(y_test,y_pred)
            metric_artifact = ClassificationMetricArtifact( f1_score=f1,precision_score=precision, recall_score=recall,accuracy_score=accuracy)
            logging.info(f"Best model found on training data: {best_model_detail}")
            return model_obj, metric_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e
        
        
    def initiate_model_trainer(self, ) -> ModelTrainerArtifact:
        logging.info("Entered initiate_model_trainer method of ModelTrainer class")
        """
        Method Name :   initiate_model_trainer
        Description :   This function initiates a model trainer steps
        
        Output      :   Returns model trainer artifact
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            logging.info(f"{'>>'*20} Model Training {'<<'*20}")
            train_arr = load_numpy_array_data(file_path=self.data_transformation_artifact.transformed_train_file_path)
            test_arr = load_numpy_array_data(file_path=self.data_transformation_artifact.transformed_test_file_path)
            
            best_model_detail ,metric_artifact = self.get_model_object_and_report(train=train_arr, test=test_arr)
            
            preprocessing_obj = load_object(file_path=self.data_transformation_artifact.transformed_object_file_path)


            if metric_artifact.accuracy_score < self.model_trainer_config.expected_accuracy:
                logging.info("No best model found with score more than base score")
                raise Exception("No best model found with score more than base score")

            usvisa_model = VisaModel(preprocessing_object=preprocessing_obj,
                                       trained_model_object=best_model_detail)
            logging.info("Created usvisa model object with preprocessor and model")
            logging.info("Created best model file path.")
            save_object(self.model_trainer_config.trained_model_file_path, usvisa_model)

            model_trainer_artifact = ModelTrainerArtifact(
                trained_model_file_path=self.model_trainer_config.trained_model_file_path,
                metric_artifact=metric_artifact,
            )
            logging.info(f"Model trainer artifact: {model_trainer_artifact}")
            return model_trainer_artifact
        except Exception as e:
            raise USVisaException(e, sys) from e