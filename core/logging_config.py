"""
Centralized logging configuration for all Eloqua scripts.
Logs to both console and rotating log files.
"""
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


def setup_logging(script_name="eloqua", log_dir="logs", console_level=logging.INFO, file_level=logging.DEBUG):
    """
    Set up logging to both console and file with rotation.
    
    Args:
        script_name: Name of the script (used for log filename)
        log_dir: Directory to store log files (created if doesn't exist)
        console_level: Logging level for console output (default: INFO)
        file_level: Logging level for file output (default: DEBUG)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler - shows INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                      datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler - rotating log files (10MB max, keep 5 backups)
    log_filename = os.path.join(log_dir, f"{script_name}.log")
    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(file_level)
    file_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Log the start of a new session
    logger.info("=" * 80)
    logger.info(f"Starting {script_name} - Session: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    return logger


def setup_thread_safe_logging(script_name="eloqua", log_dir="logs"):
    """
    Set up thread-safe logging for parallel processing scripts.
    Similar to setup_logging but with thread information in console output.
    
    Args:
        script_name: Name of the script (used for log filename)
        log_dir: Directory to store log files (created if doesn't exist)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler with thread info
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler with full details
    log_filename = os.path.join(log_dir, f"{script_name}.log")
    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    logger.info("=" * 80)
    logger.info(f"Starting {script_name} (parallel) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    return logger

