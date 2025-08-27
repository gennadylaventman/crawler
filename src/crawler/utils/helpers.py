"""
Utility functions and helper classes for the web crawler system.

This module provides common utility functions, data processing helpers,
and convenience methods used throughout the crawler system.
"""

import hashlib
import re
from typing import Dict, List, Any, Union
from urllib.parse import urlparse, urljoin
from pathlib import Path
import json
import yaml

from crawler.utils.exceptions import CrawlerError


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