import sys
import os


def error_message_detail(error: Exception, error_detail: sys) -> str:
    """
    Generate a detailed error message with file name, line number, and error details.
    
    Args:
        error: The exception object
        error_detail: sys module to extract traceback information
    
    Returns:
        str: Formatted error message with detailed information
    """
    _, _, exc_tb = error_detail.exc_info()
    
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno
    
    error_message = f"Error occurred in python script name [{file_name}] at line number [{line_number}] with error message [{str(error)}]"
    
    return error_message


class USVisaException(Exception):
    """
    Custom exception class that provides detailed error messages with traceback information.
    """
    
    def __init__(self, error_message: str, error_detail: sys):
        """
        Initialize CustomException with detailed error information.
        
        Args:
            error_message: The error message
            error_detail: sys module for traceback extraction
        """
        super().__init__(error_message)
        self.error_message = error_message_detail(Exception(error_message), error_detail)
    
    def __str__(self) -> str:
        """Return the detailed error message."""
        return self.error_message
