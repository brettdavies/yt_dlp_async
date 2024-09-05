# Standard Libraries
import os
import sys
import shutil
from datetime import datetime

# Logging
from loguru import logger

class LoggerConfig:
    @staticmethod
    def setup_logger(log_name: str, log_file_dir: str = "./data/log/", log_level: str = "INFO", log_file_prefix: str = "video_") -> None:
        """
        Set up the logger for the specified script.

        Args:
            log_name (str): The name of the log file. Typically the name of the script.
            log_file_dir (str, optional): The directory where the log file will be stored. Defaults to "./data/log/".
            log_level (str, optional): The logging level. Defaults to "INFO".
            log_file_prefix (str, optional): The prefix for the log file name. Defaults to "video_".
        """
        log_file_name = f"{log_file_prefix}{log_name}.log"
        log_file_path = os.path.join(log_file_dir, log_file_name)

        # Ensure the log directory exists
        try:
            os.makedirs(log_file_dir, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create log directory {log_file_dir}: {e}")
            raise

        # Check if the log file exists
        if os.path.exists(log_file_path):
            # Create a new name for the old log file with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_log_file_path = os.path.join(log_file_dir, f"{log_file_prefix}{log_name}_{timestamp}.log")
            try:
                # Rename the old log file
                shutil.move(log_file_path, new_log_file_path)
            except OSError as e:
                logger.error(f"Failed to rename log file {log_file_path} to {new_log_file_path}: {e}")
                raise

        # Remove all existing handlers
        logger.remove()

        # Add a logger for the screen (stderr)
        logger.add(sys.stderr, format="{time} - {level} - {message}", level=log_level)

        # Add a logger for the log file
        logger.add(log_file_path, format="{time} - {level} - {message}", level=log_level)
