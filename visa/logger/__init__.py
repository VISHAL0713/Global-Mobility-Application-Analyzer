import logging
import os
from datetime import datetime
from from_root import from_root

LOG_FILE = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logs_dir = os.path.join(from_root(), "logs")
log_file_path = os.path.join(logs_dir, LOG_FILE)
os.makedirs(logs_dir, exist_ok=True)

logging.basicConfig(
    filename=log_file_path,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    level=logging.INFO
)