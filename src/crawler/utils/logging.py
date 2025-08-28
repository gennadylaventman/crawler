"""
Simple logging configuration for the web crawler system.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any


def setup_logging(config: Optional[Dict[str, Any]] = None) -> None:
    """
    Setup basic logging configuration for the crawler system.
    
    Args:
        config: Optional logging configuration dictionary
    """
    config = config or {}
    
    # Create logs directory if it doesn't exist
    log_dir = Path(config.get('log_directory', 'logs'))
    log_dir.mkdir(exist_ok=True)
    
    # Get log level
    log_level = getattr(logging, config.get('log_level', 'INFO').upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    if config.get('console_logging', True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handler
    if config.get('file_logging', True):
        log_file = log_dir / 'crawler.log'
        try:
            file_handler = logging.FileHandler(str(log_file), encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            print(f"Warning: Could not setup file logging: {e}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f'crawler.{name}')


# Default configuration
DEFAULT_LOGGING_CONFIG = {
    'console_logging': True,
    'file_logging': True,
    'log_level': 'INFO',
    'log_directory': 'logs'
}