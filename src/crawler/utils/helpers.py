"""
Utility functions and helper classes for the web crawler system.

This module provides common utility functions, data processing helpers,
and convenience methods used throughout the crawler system.
"""

import hashlib
import re
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Callable, TypeVar, Generic
from urllib.parse import urlparse, urljoin
from pathlib import Path
import json
import yaml

from crawler.utils.exceptions import CrawlerError
from crawler.monitoring.logger import get_logger

T = TypeVar('T')


class Timer:
    """Simple timer utility for measuring execution time."""
    
    def __init__(self):
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    def start(self) -> None:
        """Start the timer."""
        self.start_time = time.perf_counter()
        self.end_time = None
    
    def stop(self) -> float:
        """Stop the timer and return elapsed time."""
        if self.start_time is None:
            raise ValueError("Timer not started")
        
        self.end_time = time.perf_counter()
        return self.elapsed
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0.0
        
        end = self.end_time or time.perf_counter()
        return end - self.start_time
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


class RateLimiter:
    """Rate limiter for controlling request frequency."""
    
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: List[float] = []
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire permission to make a request."""
        async with self._lock:
            current_time = time.time()
            
            # Remove old requests outside the time window
            cutoff_time = current_time - self.time_window
            self.requests = [req_time for req_time in self.requests if req_time > cutoff_time]
            
            # Check if we can make a request
            if len(self.requests) >= self.max_requests:
                # Calculate wait time
                oldest_request = min(self.requests)
                wait_time = self.time_window - (current_time - oldest_request)
                
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    # Recursively try again
                    await self.acquire()
                    return
            
            # Record this request
            self.requests.append(current_time)
    
    def reset(self) -> None:
        """Reset the rate limiter."""
        self.requests.clear()


class URLUtils:
    """Utility functions for URL processing."""
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """Normalize URL for consistent processing."""
        try:
            # Parse URL
            parsed = urlparse(url)
            
            # Normalize scheme and netloc
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()
            
            # Remove default ports
            if netloc.endswith(':80') and scheme == 'http':
                netloc = netloc[:-3]
            elif netloc.endswith(':443') and scheme == 'https':
                netloc = netloc[:-4]
            
            # Normalize path
            path = parsed.path or '/'
            if path != '/' and path.endswith('/'):
                path = path[:-1]
            
            # Reconstruct URL
            normalized = f"{scheme}://{netloc}{path}"
            
            if parsed.query:
                normalized += f"?{parsed.query}"
            
            return normalized
            
        except Exception:
            return url
    
    @staticmethod
    def get_domain(url: str) -> str:
        """Extract domain from URL."""
        try:
            return urlparse(url).netloc.lower()
        except:
            return ""
    
    @staticmethod
    def is_same_domain(url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        return URLUtils.get_domain(url1) == URLUtils.get_domain(url2)
    
    @staticmethod
    def resolve_relative_url(base_url: str, relative_url: str) -> str:
        """Resolve relative URL against base URL."""
        try:
            return urljoin(base_url, relative_url)
        except:
            return relative_url
    
    @staticmethod
    def get_url_hash(url: str, algorithm: str = 'md5') -> str:
        """Generate hash for URL."""
        normalized_url = URLUtils.normalize_url(url)
        
        if algorithm == 'md5':
            return hashlib.md5(normalized_url.encode()).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(normalized_url.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")


class TextUtils:
    """Utility functions for text processing."""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove non-printable characters
        text = re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', text)
        
        # Remove excessive punctuation
        text = re.sub(r'[.]{3,}', '...', text)
        text = re.sub(r'[-]{3,}', '---', text)
        
        return text.strip()
    
    @staticmethod
    def extract_words(text: str, min_length: int = 2, max_length: int = 50) -> List[str]:
        """Extract words from text with length filtering."""
        if not text:
            return []
        
        # Find all words
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        
        # Filter by length
        return [word for word in words if min_length <= len(word) <= max_length]
    
    @staticmethod
    def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
        """Truncate text to maximum length."""
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def count_sentences(text: str) -> int:
        """Count sentences in text."""
        if not text:
            return 0
        
        # Simple sentence counting
        sentences = re.split(r'[.!?]+', text)
        return len([s for s in sentences if s.strip()])
    
    @staticmethod
    def estimate_reading_time(text: str, words_per_minute: int = 200) -> int:
        """Estimate reading time in minutes."""
        if not text:
            return 0
        
        word_count = len(TextUtils.extract_words(text))
        return max(1, word_count // words_per_minute)


class DataUtils:
    """Utility functions for data processing."""
    
    @staticmethod
    def safe_divide(numerator: Union[int, float], denominator: Union[int, float], default: float = 0.0) -> float:
        """Safely divide two numbers, returning default if denominator is zero."""
        try:
            if denominator == 0:
                return default
            return numerator / denominator
        except (TypeError, ZeroDivisionError):
            return default
    
    @staticmethod
    def calculate_percentage(part: Union[int, float], total: Union[int, float]) -> float:
        """Calculate percentage with safe division."""
        return DataUtils.safe_divide(part * 100, total, 0.0)
    
    @staticmethod
    def format_bytes(bytes_value: int) -> str:
        """Format bytes in human-readable format."""
        value = float(bytes_value)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if value < 1024.0:
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{value:.1f} PB"
    
    @staticmethod
    def format_duration(seconds: float) -> str:
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    @staticmethod
    def merge_dictionaries(*dicts: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple dictionaries, with later ones taking precedence."""
        result = {}
        for d in dicts:
            if d:
                result.update(d)
        return result
    
    @staticmethod
    def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(DataUtils.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


class FileUtils:
    """Utility functions for file operations."""
    
    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> Path:
        """Ensure directory exists, creating it if necessary."""
        dir_path = Path(path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path
    
    @staticmethod
    def load_json(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load JSON data from file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise CrawlerError(f"Failed to load JSON from {file_path}: {e}")
    
    @staticmethod
    def save_json(data: Dict[str, Any], file_path: Union[str, Path], indent: int = 2) -> None:
        """Save data to JSON file."""
        try:
            FileUtils.ensure_directory(Path(file_path).parent)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, default=str, ensure_ascii=False)
        except Exception as e:
            raise CrawlerError(f"Failed to save JSON to {file_path}: {e}")
    
    @staticmethod
    def load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load YAML data from file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            raise CrawlerError(f"Failed to load YAML from {file_path}: {e}")
    
    @staticmethod
    def save_yaml(data: Dict[str, Any], file_path: Union[str, Path]) -> None:
        """Save data to YAML file."""
        try:
            FileUtils.ensure_directory(Path(file_path).parent)
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
        except Exception as e:
            raise CrawlerError(f"Failed to save YAML to {file_path}: {e}")
    
    @staticmethod
    def get_file_size(file_path: Union[str, Path]) -> int:
        """Get file size in bytes."""
        try:
            return Path(file_path).stat().st_size
        except Exception:
            return 0
    
    @staticmethod
    def backup_file(file_path: Union[str, Path], backup_suffix: str = '.bak') -> Path:
        """Create backup of file."""
        source_path = Path(file_path)
        backup_path = source_path.with_suffix(source_path.suffix + backup_suffix)
        
        try:
            if source_path.exists():
                backup_path.write_bytes(source_path.read_bytes())
            return backup_path
        except Exception as e:
            raise CrawlerError(f"Failed to backup file {file_path}: {e}")


class AsyncUtils:
    """Utility functions for async operations."""
    
    @staticmethod
    async def run_with_timeout(coro, timeout: float, default=None):
        """Run coroutine with timeout, returning default on timeout."""
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            return default
    
    @staticmethod
    async def gather_with_limit(coroutines: List, limit: int = 10):
        """Run coroutines with concurrency limit."""
        semaphore = asyncio.Semaphore(limit)
        
        async def limited_coro(coro):
            async with semaphore:
                return await coro
        
        limited_coroutines = [limited_coro(coro) for coro in coroutines]
        return await asyncio.gather(*limited_coroutines, return_exceptions=True)
    
    @staticmethod
    async def retry_async(
        func: Callable,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff_factor: float = 2.0,
        exceptions: tuple = (Exception,)
    ):
        """Retry async function with exponential backoff."""
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                return await func()
            except exceptions as e:
                last_exception = e
                
                if attempt < max_retries:
                    wait_time = delay * (backoff_factor ** attempt)
                    await asyncio.sleep(wait_time)
                else:
                    break
        
        if last_exception:
            raise last_exception
        else:
            raise CrawlerError("Retry failed with no exception recorded")


class ValidationUtils:
    """Utility functions for data validation."""
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if URL is valid."""
        try:
            parsed = urlparse(url)
            return bool(parsed.scheme and parsed.netloc)
        except:
            return False
    
    @staticmethod
    def is_valid_email(email: str) -> bool:
        """Check if email is valid."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_config(config: Dict[str, Any], required_keys: List[str]) -> List[str]:
        """Validate configuration dictionary."""
        missing_keys = []
        for key in required_keys:
            if key not in config:
                missing_keys.append(key)
        return missing_keys
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitize filename for safe file system usage."""
        # Remove or replace invalid characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Remove leading/trailing dots and spaces
        sanitized = sanitized.strip('. ')
        
        # Limit length
        if len(sanitized) > 255:
            sanitized = sanitized[:255]
        
        return sanitized or 'unnamed'


class CacheUtils:
    """Utility functions for caching operations."""
    
    @staticmethod
    def create_cache_key(*args, **kwargs) -> str:
        """Create cache key from arguments."""
        key_parts = []
        
        # Add positional arguments
        for arg in args:
            key_parts.append(str(arg))
        
        # Add keyword arguments
        for key, value in sorted(kwargs.items()):
            key_parts.append(f"{key}={value}")
        
        # Create hash of combined key
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    @staticmethod
    def is_cache_valid(cache_time: datetime, ttl_seconds: int) -> bool:
        """Check if cache entry is still valid."""
        if not cache_time:
            return False
        
        expiry_time = cache_time + timedelta(seconds=ttl_seconds)
        return datetime.utcnow() < expiry_time


class ConfigUtils:
    """Utility functions for configuration management."""
    
    @staticmethod
    def load_config_file(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load configuration from file (JSON or YAML)."""
        path = Path(file_path)
        
        if not path.exists():
            raise CrawlerError(f"Configuration file not found: {file_path}")
        
        if path.suffix.lower() in ['.yml', '.yaml']:
            return FileUtils.load_yaml(path)
        elif path.suffix.lower() == '.json':
            return FileUtils.load_json(path)
        else:
            raise CrawlerError(f"Unsupported configuration file format: {path.suffix}")
    
    @staticmethod
    def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple configuration dictionaries."""
        result = {}
        
        for config in configs:
            if not config:
                continue
            
            for key, value in config.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    # Recursively merge nested dictionaries
                    result[key] = ConfigUtils.merge_configs(result[key], value)
                else:
                    result[key] = value
        
        return result
    
    @staticmethod
    def apply_environment_overrides(config: Dict[str, Any], prefix: str = 'CRAWLER_') -> Dict[str, Any]:
        """Apply environment variable overrides to configuration."""
        import os
        
        result = config.copy()
        
        for env_key, env_value in os.environ.items():
            if env_key.startswith(prefix):
                # Convert environment variable name to config key
                config_key = env_key[len(prefix):].lower().replace('_', '.')
                
                # Set nested configuration value
                ConfigUtils._set_nested_value(result, config_key, env_value)
        
        return result
    
    @staticmethod
    def _set_nested_value(config: Dict[str, Any], key_path: str, value: str) -> None:
        """Set nested configuration value using dot notation."""
        keys = key_path.split('.')
        current = config
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        # Convert string value to appropriate type
        final_key = keys[-1]
        current[final_key] = ConfigUtils._convert_env_value(value)
    
    @staticmethod
    def _convert_env_value(value: str) -> Union[str, int, float, bool]:
        """Convert environment variable string to appropriate type."""
        # Boolean values
        if value.lower() in ('true', 'yes', '1', 'on'):
            return True
        elif value.lower() in ('false', 'no', '0', 'off'):
            return False
        
        # Numeric values
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # String value
        return value


class PerformanceUtils:
    """Utility functions for performance monitoring and optimization."""
    
    @staticmethod
    def measure_memory_usage() -> Dict[str, float]:
        """Measure current memory usage."""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'percent': process.memory_percent()
            }
        except Exception:
            return {'rss_mb': 0, 'vms_mb': 0, 'percent': 0}
    
    @staticmethod
    def measure_cpu_usage() -> float:
        """Measure current CPU usage."""
        try:
            import psutil
            return psutil.cpu_percent(interval=0.1)
        except Exception:
            return 0.0
    
    @staticmethod
    async def profile_async_function(func: Callable, *args, **kwargs) -> Dict[str, Any]:
        """Profile async function execution."""
        start_time = time.perf_counter()
        start_memory = PerformanceUtils.measure_memory_usage()
        
        try:
            result = await func(*args, **kwargs)
            success = True
            error = None
        except Exception as e:
            result = None
            success = False
            error = str(e)
        
        end_time = time.perf_counter()
        end_memory = PerformanceUtils.measure_memory_usage()
        
        return {
            'result': result,
            'success': success,
            'error': error,
            'duration': end_time - start_time,
            'memory_start_mb': start_memory['rss_mb'],
            'memory_end_mb': end_memory['rss_mb'],
            'memory_delta_mb': end_memory['rss_mb'] - start_memory['rss_mb']
        }


class LoggingUtils:
    """Utility functions for logging operations."""
    
    @staticmethod
    def create_log_context(
        session_id: Optional[str] = None,
        worker_id: Optional[int] = None,
        url: Optional[str] = None,
        **extra
    ) -> Dict[str, Any]:
        """Create logging context dictionary."""
        context = {}
        
        if session_id:
            context['session_id'] = session_id
        if worker_id is not None:
            context['worker_id'] = worker_id
        if url:
            context['url'] = url
            context['domain'] = URLUtils.get_domain(url)
        
        context.update(extra)
        return context
    
    @staticmethod
    def format_log_message(message: str, **context) -> str:
        """Format log message with context."""
        if not context:
            return message
        
        context_str = " | ".join(f"{k}={v}" for k, v in context.items())
        return f"{message} | {context_str}"


# Convenience functions
def get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.utcnow().isoformat() + 'Z'


def generate_session_id(prefix: str = 'crawl') -> str:
    """Generate unique session ID."""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    random_suffix = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
    return f"{prefix}_{timestamp}_{random_suffix}"


def chunks(lst: List[T], chunk_size: int) -> List[List[T]]:
    """Split list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def deduplicate_list(lst: List[T], key_func: Optional[Callable[[T], Any]] = None) -> List[T]:
    """Remove duplicates from list while preserving order."""
    seen = set()
    result = []
    
    for item in lst:
        key = key_func(item) if key_func else item
        if key not in seen:
            seen.add(key)
            result.append(item)
    
    return result