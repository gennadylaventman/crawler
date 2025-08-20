"""
Configuration management for the web crawler system.
"""

import os
import yaml
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, validator
from pathlib import Path


class DatabaseConfig(BaseModel):
    """Database configuration settings."""
    host: str = "localhost"
    port: int = 5432
    database: str = "webcrawler"
    username: str = "crawler"
    password: str = "password"
    pool_size: int = 20
    max_overflow: int = 10
    pool_timeout: int = 30
    
    @property
    def url(self) -> str:
        """Get database URL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class CrawlerConfig(BaseModel):
    """Crawler configuration settings."""
    max_depth: int = 3
    max_pages: int = 1000
    concurrent_workers: int = 10
    rate_limit_delay: float = 0.1
    request_timeout: int = 30
    max_retries: int = 3
    user_agent: str = "WebCrawler/1.0 (+https://example.com/bot)"
    
    # Connection settings
    max_connections: int = 100
    max_connections_per_host: int = 20
    dns_cache_ttl: int = 300
    keepalive_timeout: int = 30
    
    # Queue settings
    url_queue_size: int = 100000
    enable_bloom_filter: bool = True
    
    # Persistent queue settings
    enable_persistent_queue: bool = False
    queue_max_retries: int = 3
    
    @validator('concurrent_workers')
    def validate_workers(cls, v):
        if v < 1 or v > 200:
            raise ValueError('concurrent_workers must be between 1 and 200')
        return v
    
    @validator('rate_limit_delay')
    def validate_rate_limit(cls, v):
        if v < 0:
            raise ValueError('rate_limit_delay must be non-negative')
        return v


class ContentConfig(BaseModel):
    """Content processing configuration."""
    max_page_size: int = 10485760  # 10MB
    allowed_content_types: List[str] = [
        "text/html",
        "application/xhtml+xml"
    ]
    
    # Text extraction settings
    remove_scripts: bool = True
    remove_styles: bool = True
    min_text_length: int = 100
    max_words_per_page: int = 50000


class CrawlConfig(BaseModel):
    """Complete crawler configuration."""
    database: DatabaseConfig
    crawler: CrawlerConfig
    content: ContentConfig
    
    # Session-specific settings
    session_name: str = "default_crawl"
    start_urls: List[str] = []
    allowed_domains: Optional[List[str]] = None
    blocked_domains: Optional[List[str]] = None
    
    @validator('start_urls')
    def validate_start_urls(cls, v):
        # Allow empty start_urls initially - they can be set later via CLI
        return v


class ConfigManager:
    """Configuration manager for loading and validating settings."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self._config: Optional[CrawlConfig] = None
    
    def _find_config_file(self) -> str:
        """Find configuration file in standard locations."""
        possible_paths = [
            "config/default.yaml",
            "config.yaml",
            "crawler.yaml",
            os.path.expanduser("~/.crawler/config.yaml")
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # Return default path if none found
        return "config/default.yaml"
    
    def load_config(self, environment: str = "default") -> CrawlConfig:
        """Load configuration from file."""
        if self._config is not None:
            return self._config
        
        config_data = self._load_yaml_config()
        
        # Apply environment-specific overrides
        if environment != "default" and environment in config_data:
            final_config = config_data[environment]
        else:
            # Use default config or fallback to entire config if no default section
            final_config = config_data.get("default", config_data)
        
        # Apply environment variable overrides
        self._apply_env_overrides(final_config)
        
        self._config = CrawlConfig(**final_config)
        return self._config
    
    def _load_yaml_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Return default configuration if file not found
            return self._get_default_config()
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "webcrawler",
                "username": "crawler",
                "password": os.getenv("DB_PASSWORD", "password"),
                "pool_size": 20
            },
            "crawler": {
                "max_depth": 3,
                "max_pages": 1000,
                "concurrent_workers": 10,
                "rate_limit_delay": 1.0,
                "request_timeout": 30,
                "max_retries": 3,
                "enable_persistent_queue": False,
                "queue_max_retries": 3
            },
            "content": {
                "max_page_size": 10485760,
                "allowed_content_types": ["text/html", "application/xhtml+xml"],
                "remove_scripts": True,
                "remove_styles": True,
                "min_text_length": 100
            },
            "session_name": "default_crawl",
            "start_urls": []
        }
    
    def _merge_config(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        """Merge configuration dictionaries."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> None:
        """Apply environment variable overrides."""
        env_mappings = {
            "DB_HOST": ("database", "host"),
            "DB_PORT": ("database", "port"),
            "DB_NAME": ("database", "database"),
            "DB_USER": ("database", "username"),
            "DB_PASSWORD": ("database", "password"),
            "CRAWLER_MAX_DEPTH": ("crawler", "max_depth"),
            "CRAWLER_MAX_PAGES": ("crawler", "max_pages"),
            "CRAWLER_WORKERS": ("crawler", "concurrent_workers"),
            "CRAWLER_RATE_LIMIT": ("crawler", "rate_limit_delay"),
            "CRAWLER_ENABLE_PERSISTENT_QUEUE": ("crawler", "enable_persistent_queue"),
            "CRAWLER_QUEUE_MAX_RETRIES": ("crawler", "queue_max_retries"),
            "LOG_LEVEL": ("monitoring", "log_level"),
        }
        
        for env_var, (section, key) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                if section not in config:
                    config[section] = {}
                
                # Clean the value by removing inline comments and extra whitespace
                # Split on '#' and take the first part, then strip whitespace
                clean_value = value.split('#')[0].strip()
                
                # Convert to appropriate type
                if key in ["port", "max_depth", "max_pages", "concurrent_workers",
                          "queue_sync_interval", "queue_batch_size", "queue_cleanup_interval",
                          "queue_recovery_timeout", "queue_max_retries"]:
                    config[section][key] = int(clean_value)
                elif key in ["rate_limit_delay"]:
                    config[section][key] = float(clean_value)
                elif key in ["enable_persistent_queue"]:
                    # Handle boolean conversion for persistent queue setting
                    config[section][key] = clean_value.lower() in ('true', '1', 'yes', 'on')
                else:
                    config[section][key] = clean_value
    
    def save_config(self, config: CrawlConfig, path: Optional[str] = None) -> None:
        """Save configuration to file."""
        save_path = path or self.config_path
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Convert to dictionary and save
        config_dict = config.dict()
        with open(save_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
    
    def validate_config(self, config_dict: Dict[str, Any]) -> bool:
        """Validate configuration dictionary."""
        try:
            CrawlConfig(**config_dict)
            return True
        except Exception:
            return False
    
    @property
    def config(self) -> Optional[CrawlConfig]:
        """Get current configuration."""
        return self._config


# Global configuration instance
config_manager = ConfigManager(os.environ['CRAWLER_CONFIG_PATH'] if 'CRAWLER_CONFIG_PATH' in os.environ else None)


def get_config(environment: str = "default") -> CrawlConfig:
    """Get configuration instance."""
    return config_manager.load_config(environment)


def reload_config() -> CrawlConfig:
    """Reload configuration from file."""
    config_manager._config = None
    return config_manager.load_config()