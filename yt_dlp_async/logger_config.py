# Standard Libraries
import os
import sys
import shutil
from datetime import datetime

# Logging
from loguru import logger

class LoggerConfig:
    """
    Set up the logger for the specified script.

    Parameters:
    - script_name (str): The name of the script.
    - log_file_dir (str): The directory where the log file will be stored. Default is "../data/log/".

    Returns:
    - None
    """
    @staticmethod
    def setup_logger(script_name: str, log_file_dir: str = "../data/log/") -> None:
        """
        Set up the logger for the specified script.

        Args:
            script_name (str): The name of the script.
            log_file_dir (str, optional): The directory where the log file will be stored. Defaults to "../data/log/".

        Returns:
            None
        """
        log_file_name = f"video_{script_name}.log"
        log_file_path = os.path.join(log_file_dir, log_file_name)

        # Ensure the log directory exists
        os.makedirs(log_file_dir, exist_ok=True)

        # Check if the log file exists
        if os.path.exists(log_file_path):
            # Create a new name for the old log file with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_log_file_path = os.path.join(log_file_dir, f"video_{script_name}_{timestamp}.log")
            # Rename the old log file
            shutil.move(log_file_path, new_log_file_path)

        # Remove all existing handlers
        logger.remove()

        # Add a logger for the screen (stderr)
        logger.add(sys.stderr, format="{time} - {level} - {message}", level="INFO")

        # Add a logger for the log file
        logger.add(log_file_path, format="{time} - {level} - {message}", level="INFO")
