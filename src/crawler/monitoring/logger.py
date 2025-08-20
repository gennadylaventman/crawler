"""
Logging configuration and utilities for the web crawler system.

This module provides structured logging with JSON formatting, log rotation,
performance-optimized logging, and different log levels for various components.
"""

import logging
import logging.handlers
import json
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional, Union
from pathlib import Path
from contextlib import contextmanager

from crawler.utils.exceptions import LoggingError


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    """
    
    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        # Base log data
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'thread_name': record.threadName,
            'process': record.process,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info) if record.exc_info else None
            }
        
        # Add extra fields if enabled
        if self.include_extra:
            # Add any extra fields from the log record
            extra_fields = {
                key: value for key, value in record.__dict__.items()
                if key not in {
                    'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                    'filename', 'module', 'lineno', 'funcName', 'created',
                    'msecs', 'relativeCreated', 'thread', 'threadName',
                    'processName', 'process', 'getMessage', 'exc_info',
                    'exc_text', 'stack_info', 'message'
                }
            }
            
            if extra_fields:
                log_data['extra'] = extra_fields
        
        return json.dumps(log_data, default=str, ensure_ascii=False)


class CrawlerLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds crawler-specific context to log records.
    """
    
    def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None):
        super().__init__(logger, extra or {})
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process log message and add extra context."""
        # Merge extra context
        if 'extra' in kwargs:
            kwargs['extra'].update(self.extra)
        else:
            kwargs['extra'] = dict(self.extra) if self.extra else {}
        
        return msg, kwargs
    
    def with_context(self, **context) -> 'CrawlerLoggerAdapter':
        """Create new adapter with additional context."""
        new_extra = dict(self.extra) if self.extra else {}
        new_extra.update(context)
        return CrawlerLoggerAdapter(self.logger, new_extra)


class PerformanceLogger:
    """
    Performance-optimized logger for high-frequency operations.
    """
    
    def __init__(self, logger: logging.Logger, batch_size: int = 100):
        self.logger = logger
        self.batch_size = batch_size
        self._batch = []
        self._enabled = logger.isEnabledFor(logging.INFO)
    
    def log_batch(self, level: int, messages: list) -> None:
        """Log multiple messages in batch."""
        if not self._enabled:
            return
        
        for msg in messages:
            if isinstance(msg, dict):
                self.logger.log(level, msg.get('message', ''), extra=msg.get('extra', {}))
            else:
                self.logger.log(level, str(msg))
    
    def add_to_batch(self, level: int, message: str, **extra) -> None:
        """Add message to batch for later logging."""
        if not self._enabled:
            return
        
        self._batch.append({
            'level': level,
            'message': message,
            'extra': extra
        })
        
        if len(self._batch) >= self.batch_size:
            self.flush_batch()
    
    def flush_batch(self) -> None:
        """Flush accumulated batch messages."""
        if not self._batch:
            return
        
        for item in self._batch:
            self.logger.log(item['level'], item['message'], extra=item['extra'])
        
        self._batch.clear()


class LoggerManager:
    """
    Centralized logger management for the crawler system.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self._loggers: Dict[str, logging.Logger] = {}
        self._handlers: Dict[str, logging.Handler] = {}
        self._setup_root_logger()
    
    def _setup_root_logger(self) -> None:
        """Setup root logger configuration."""
        root_logger = logging.getLogger('crawler')
        root_logger.setLevel(self._get_log_level('root', 'INFO'))
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add console handler
        if self.config.get('console_logging', True):
            console_handler = self._create_console_handler()
            root_logger.addHandler(console_handler)
        
        # Add file handler
        if self.config.get('file_logging', True):
            file_handler = self._create_file_handler()
            if file_handler:
                root_logger.addHandler(file_handler)
        
        # Add error file handler
        if self.config.get('error_file_logging', True):
            error_handler = self._create_error_file_handler()
            if error_handler:
                root_logger.addHandler(error_handler)
        
        # Prevent propagation to avoid duplicate logs
        root_logger.propagate = False
    
    def _create_console_handler(self) -> logging.Handler:
        """Create console handler with appropriate formatting."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(self._get_log_level('console', 'INFO'))
        
        if self.config.get('json_format', False):
            formatter = JSONFormatter()
        else:
            formatter = logging.Formatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        handler.setFormatter(formatter)
        self._handlers['console'] = handler
        return handler
    
    def _create_file_handler(self) -> Optional[logging.Handler]:
        """Create rotating file handler."""
        log_dir = Path(self.config.get('log_directory', 'logs'))
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / 'crawler.log'
        
        try:
            handler = logging.handlers.RotatingFileHandler(
                filename=str(log_file),
                maxBytes=self.config.get('max_file_size', 10 * 1024 * 1024),  # 10MB
                backupCount=self.config.get('backup_count', 5),
                encoding='utf-8'
            )
            
            handler.setLevel(self._get_log_level('file', 'DEBUG'))
            
            if self.config.get('json_format', True):
                formatter = JSONFormatter()
            else:
                formatter = logging.Formatter(
                    fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
            
            handler.setFormatter(formatter)
            self._handlers['file'] = handler
            return handler
            
        except Exception as e:
            print(f"Failed to create file handler: {e}")
            return None
    
    def _create_error_file_handler(self) -> Optional[logging.Handler]:
        """Create separate handler for error logs."""
        log_dir = Path(self.config.get('log_directory', 'logs'))
        log_dir.mkdir(exist_ok=True)
        
        error_log_file = log_dir / 'crawler_errors.log'
        
        try:
            handler = logging.handlers.RotatingFileHandler(
                filename=str(error_log_file),
                maxBytes=self.config.get('max_error_file_size', 5 * 1024 * 1024),  # 5MB
                backupCount=self.config.get('error_backup_count', 3),
                encoding='utf-8'
            )
            
            handler.setLevel(logging.ERROR)
            formatter = JSONFormatter()
            handler.setFormatter(formatter)
            
            self._handlers['error_file'] = handler
            return handler
            
        except Exception as e:
            print(f"Failed to create error file handler: {e}")
            return None
    
    def _get_log_level(self, handler_type: str, default: str) -> int:
        """Get log level for specific handler type."""
        level_name = self.config.get(f'{handler_type}_log_level', default).upper()
        return getattr(logging, level_name, logging.INFO)
    
    def get_logger(self, name: str, **context) -> CrawlerLoggerAdapter:
        """Get logger with optional context."""
        if name not in self._loggers:
            logger = logging.getLogger(f'crawler.{name}')
            logger.setLevel(self._get_log_level(name, 'INFO'))
            self._loggers[name] = logger
        
        return CrawlerLoggerAdapter(self._loggers[name], context)
    
    def get_performance_logger(self, name: str, batch_size: int = 100) -> PerformanceLogger:
        """Get performance-optimized logger."""
        logger = self.get_logger(name).logger
        return PerformanceLogger(logger, batch_size)
    
    def set_log_level(self, logger_name: str, level: Union[str, int]) -> None:
        """Set log level for specific logger."""
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
        
        if logger_name in self._loggers:
            self._loggers[logger_name].setLevel(level)
    
    def add_handler(self, handler_name: str, handler: logging.Handler) -> None:
        """Add custom handler."""
        self._handlers[handler_name] = handler
        
        # Add to all existing loggers
        for logger in self._loggers.values():
            logger.addHandler(handler)
    
    def remove_handler(self, handler_name: str) -> None:
        """Remove handler."""
        if handler_name in self._handlers:
            handler = self._handlers.pop(handler_name)
            
            # Remove from all loggers
            for logger in self._loggers.values():
                logger.removeHandler(handler)
            
            handler.close()
    
    def shutdown(self) -> None:
        """Shutdown all handlers and loggers."""
        for handler in self._handlers.values():
            handler.close()
        
        self._handlers.clear()
        self._loggers.clear()
        
        # Shutdown logging
        logging.shutdown()


# Global logger manager instance
_logger_manager: Optional[LoggerManager] = None


def setup_logging(config: Optional[Dict[str, Any]] = None) -> LoggerManager:
    """
    Setup logging configuration for the crawler system.
    
    Args:
        config: Logging configuration dictionary
        
    Returns:
        LoggerManager instance
    """
    global _logger_manager
    
    if _logger_manager is not None:
        _logger_manager.shutdown()
    
    _logger_manager = LoggerManager(config)
    return _logger_manager


def get_logger(name: str, **context) -> CrawlerLoggerAdapter:
    """
    Get logger instance with optional context.
    
    Args:
        name: Logger name
        **context: Additional context to include in logs
        
    Returns:
        CrawlerLoggerAdapter instance
    """
    global _logger_manager
    
    if _logger_manager is None:
        _logger_manager = LoggerManager()
    
    return _logger_manager.get_logger(name, **context)


def get_performance_logger(name: str, batch_size: int = 100) -> PerformanceLogger:
    """
    Get performance-optimized logger.
    
    Args:
        name: Logger name
        batch_size: Batch size for performance logging
        
    Returns:
        PerformanceLogger instance
    """
    global _logger_manager
    
    if _logger_manager is None:
        _logger_manager = LoggerManager()
    
    return _logger_manager.get_performance_logger(name, batch_size)


@contextmanager
def log_execution_time(logger: logging.Logger, operation: str, level: int = logging.INFO):
    """
    Context manager to log execution time of operations.
    
    Args:
        logger: Logger instance
        operation: Operation description
        level: Log level
    """
    start_time = datetime.utcnow()
    
    try:
        logger.log(level, f"Starting {operation}")
        yield
        
    except Exception as e:
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.error(
            f"Failed {operation} after {duration:.3f}s",
            extra={'operation': operation, 'duration': duration, 'error': str(e)}
        )
        raise
        
    else:
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.log(
            level,
            f"Completed {operation} in {duration:.3f}s",
            extra={'operation': operation, 'duration': duration}
        )


class LoggingMixin:
    """
    Mixin class to add logging capabilities to other classes.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = None
    
    @property
    def logger(self) -> CrawlerLoggerAdapter:
        """Get logger for this class."""
        if self._logger is None:
            class_name = self.__class__.__name__.lower()
            self._logger = get_logger(class_name, class_name=self.__class__.__name__)
        return self._logger
    
    def log_with_context(self, level: int, message: str, **context) -> None:
        """Log message with additional context."""
        self.logger.log(level, message, extra=context)


# Convenience functions for common log levels
def debug(message: str, logger_name: str = 'main', **context) -> None:
    """Log debug message."""
    get_logger(logger_name).debug(message, extra=context)


def info(message: str, logger_name: str = 'main', **context) -> None:
    """Log info message."""
    get_logger(logger_name).info(message, extra=context)


def warning(message: str, logger_name: str = 'main', **context) -> None:
    """Log warning message."""
    get_logger(logger_name).warning(message, extra=context)


def error(message: str, logger_name: str = 'main', **context) -> None:
    """Log error message."""
    get_logger(logger_name).error(message, extra=context)


def critical(message: str, logger_name: str = 'main', **context) -> None:
    """Log critical message."""
    get_logger(logger_name).critical(message, extra=context)


# Default logging configuration
DEFAULT_LOGGING_CONFIG = {
    'console_logging': True,
    'file_logging': True,
    'error_file_logging': True,
    'json_format': True,
    'log_directory': 'logs',
    'max_file_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5,
    'max_error_file_size': 5 * 1024 * 1024,  # 5MB
    'error_backup_count': 3,
    'root_log_level': 'INFO',
    'console_log_level': 'INFO',
    'file_log_level': 'DEBUG',
    'engine_log_level': 'INFO',
    'worker_log_level': 'INFO',
    'database_log_level': 'INFO',
    'metrics_log_level': 'INFO',
    'content_log_level': 'INFO',
    'url_management_log_level': 'INFO',
    'monitoring_log_level': 'INFO'
}