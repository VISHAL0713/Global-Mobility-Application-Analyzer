import numpy as np
import pandas as pd
import dill
import yaml
from pandas import DataFrame
import sys
import os

from visa.logger import logging 
from visa.exception import USVisaException


def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its contents as a dictionary.
    
    Args:
        file_path (str): The path to the YAML file.
    Returns:
        dict: The contents of the YAML file as a dictionary.        
    Raises:
        USVisaException: If there is an error reading the YAML file.    
    """
    try:
        with open(file_path,"rb") as yaml_file:
            return yaml.safe_load(yaml_file)
        
    except Exception as e:
        raise USVisaException(e, sys) from e
    
    
def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """Writes a Python object to a YAML file.
    Args:
        file_path (str): The path to the YAML file.
        content (object): The Python object to be written to the YAML file.
        replace (bool, optional): Whether to replace the file if it already exists. Defaults to False.
    Raises:        USVisaException: If there is an error writing to the YAML file.
    """
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
                
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content,file)
            
    except Exception as e:
        raise USVisaException(e, sys) from e
    
    
def load_object(file_path: str) -> object:
    """
    Loads a Python object from a file using dill.
    
    Args:
        file_path (str): The path to the file containing the serialized object. 
    Returns:
        object: The deserialized Python object.
    Raises:
        USVisaException: If there is an error loading the object from the file.
        
    """
    try:
        with open(file_path, "rb") as file_obj:
            return dill.load(file_obj)
        
    except Exception as e:
        raise USVisaException(e, sys) from e
    
    
def save_numpy_array_data(file_path: str, array: np.ndarray) -> None:
    """
    Saves a NumPy array to a file.
    
    Args:
        file_path (str): The path to the file where the array will be saved.
        array (np.ndarray): The NumPy array to be saved.
    Raises:
        USVisaException: If there is an error saving the NumPy array to the file.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
            
    except Exception as e:
        raise USVisaException(e, sys) from e
    
    
def load_numpy_array_data(file_path: str) -> np.ndarray:
    """
    Loads a NumPy array from a file.
    
    Args:
        file_path (str): The path to the file containing the saved NumPy array. 
    Returns:
        np.ndarray: The loaded NumPy array.
    Raises:
        USVisaException: If there is an error loading the NumPy array from the file.
    """
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
        
    except Exception as e:
        raise USVisaException(e, sys) from e
    
    
def save_object(file_path: str, obj: object) -> None:
    """
    Saves a Python object to a file using dill.
    
    Args:
        file_path (str): The path to the file where the object will be saved.
        obj (object): The Python object to be saved.
    Raises:
        USVisaException: If there is an error saving the object to the file.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
            
    except Exception as e:
        raise USVisaException(e, sys) from e
    
    
def drop_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Drops specified columns from a DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame from which columns will be dropped.
        columns (list): A list of column names to be dropped from the DataFrame.
    Returns:
        DataFrame: A new DataFrame with the specified columns dropped.  
    Raises:
        USVisaException: If there is an error dropping the columns from the DataFrame.
    """
    try:
        df = df.drop(columns=columns, axis=1)
        logging.info(f"Columns dropped successfully: {columns}")
        
        return df
    except Exception as e:
        raise USVisaException(e, sys) from e