from visa.logger import logging
from visa.exception import USVisaException
import sys

# logging.info("This is an info message from demo.py")
# logging.warning("This is a warning message from demo.py")

try:
    x = 1 / 0
except Exception as e:
    raise USVisaException(e, sys)