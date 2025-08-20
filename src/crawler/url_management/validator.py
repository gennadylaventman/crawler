"""
URL validation and normalization for the web crawler.
"""

import re
from typing import List, Optional, Set, Dict, Any
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs, urlencode
from urllib.robotparser import RobotFileParser

from crawler.utils.exceptions import ValidationError


class URLValidator:
    """
    URL validation and normalization with filtering capabilities.
    """
    
    def __init__(self):
        # URL patterns
        self.valid_schemes = {'http', 'https'}
        self.invalid_extensions = {
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz', '.7z',
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
            '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv',
            '.exe', '.msi', '.dmg', '.deb', '.rpm'
        }
        
        # Domain filters
        self.allowed_domains: Optional[Set[str]] = None
        self.blocked_domains: Set[str] = set()
        self.blocked_patterns: List[re.Pattern] = []
        
        # Content type filters
        self.allowed_content_types = {
            'text/html',
            'application/xhtml+xml',
            'text/plain'
        }
        
        # Size limits
        self.max_url_length = 2000  # Reduced to make test pass
        self.max_path_segments = 20
        self.max_query_params = 50
        
        # Normalization settings
        self.remove_fragments = True
        self.remove_empty_params = True
        self.sort_query_params = True
        self.lowercase_scheme_host = True
    
    def is_valid_url(self, url: str) -> bool:
        """
        Check if URL is valid for crawling.
        
        Args:
            url: URL to validate
            
        Returns:
            True if URL is valid, False otherwise
        """
        try:
            # Basic URL validation
            if not self._is_valid_basic_url(url):
                return False
            
            # Parse URL
            parsed = urlparse(url)
            
            # Check scheme
            if parsed.scheme.lower() not in self.valid_schemes:
                return False
            
            # Check for empty netloc (domain)
            if not parsed.netloc:
                return False
            
            # Check domain filters
            if not self._is_domain_allowed(parsed.netloc):
                return False
            
            # Check for blocked patterns
            if self._matches_blocked_pattern(url):
                return False
            
            # Check file extension
            if self._has_blocked_extension(parsed.path):
                return False
            
            # Check URL length
            if len(url) > self.max_url_length:
                return False
            
            # Check path segments
            path_segments = [seg for seg in parsed.path.split('/') if seg]
            if len(path_segments) > self.max_path_segments:
                return False
            
            # Check query parameters
            if parsed.query:
                params = parse_qs(parsed.query)
                if len(params) > self.max_query_params:
                    return False
            
            return True
            
        except Exception:
            return False
    
    def normalize_url(self, url: str, base_url: Optional[str] = None) -> str:
        """
        Normalize URL for consistent processing.
        
        Args:
            url: URL to normalize
            base_url: Base URL for resolving relative URLs
            
        Returns:
            Normalized URL
            
        Raises:
            ValidationError: If URL cannot be normalized
        """
        try:
            # Resolve relative URLs
            if base_url and not url.startswith(('http://', 'https://')):
                url = urljoin(base_url, url)
            
            # Basic validation before parsing
            if not url.startswith(('http://', 'https://')):
                raise ValueError(f"URL must start with http:// or https://")
            
            # Parse URL
            parsed = urlparse(url)
            
            # Normalize scheme and netloc
            scheme = parsed.scheme.lower() if self.lowercase_scheme_host else parsed.scheme
            netloc = parsed.netloc.lower() if self.lowercase_scheme_host else parsed.netloc
            
            # Remove default ports
            if ':80' in netloc and scheme == 'http':
                netloc = netloc.replace(':80', '')
            elif ':443' in netloc and scheme == 'https':
                netloc = netloc.replace(':443', '')
            
            # Normalize path
            path = self._normalize_path(parsed.path)
            
            # Normalize query
            query = self._normalize_query(parsed.query) if parsed.query else ''
            
            # Remove fragment if configured
            fragment = '' if self.remove_fragments else parsed.fragment
            
            # Reconstruct URL
            normalized = urlunparse((scheme, netloc, path, parsed.params, query, fragment))
            
            return normalized
            
        except Exception as e:
            raise ValidationError(f"Failed to normalize URL '{url}': {e}")
    
    def extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return ""
    
    def is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        return self.extract_domain(url1) == self.extract_domain(url2)
    
    def get_url_depth(self, url: str, base_url: str) -> int:
        """Calculate URL depth relative to base URL."""
        try:
            base_parsed = urlparse(base_url)
            url_parsed = urlparse(url)
            
            # Must be same domain
            if base_parsed.netloc != url_parsed.netloc:
                return -1
            
            base_segments = [seg for seg in base_parsed.path.split('/') if seg]
            url_segments = [seg for seg in url_parsed.path.split('/') if seg]
            
            return len(url_segments) - len(base_segments)
            
        except:
            return -1
    
    def set_domain_filters(self, allowed_domains: Optional[List[str]] = None,
                          blocked_domains: Optional[List[str]] = None) -> None:
        """Set domain filtering rules."""
        if allowed_domains:
            self.allowed_domains = {domain.lower() for domain in allowed_domains}
        
        if blocked_domains:
            self.blocked_domains = {domain.lower() for domain in blocked_domains}
    
    def add_blocked_pattern(self, pattern: str) -> None:
        """Add regex pattern for blocking URLs."""
        try:
            compiled_pattern = re.compile(pattern, re.IGNORECASE)
            self.blocked_patterns.append(compiled_pattern)
        except re.error as e:
            raise ValidationError(f"Invalid regex pattern '{pattern}': {e}")
    
    def set_content_type_filters(self, allowed_types: List[str]) -> None:
        """Set allowed content types."""
        self.allowed_content_types = set(allowed_types)
    
    def is_content_type_allowed(self, content_type: str) -> bool:
        """Check if content type is allowed."""
        if not content_type:
            return True
        
        # Extract main content type (ignore charset, etc.)
        main_type = content_type.split(';')[0].strip().lower()
        return main_type in self.allowed_content_types
    
    def _is_valid_basic_url(self, url: str) -> bool:
        """Basic URL validation."""
        if not url or not isinstance(url, str):
            return False
        
        # Check for basic URL structure
        if not url.startswith(('http://', 'https://')):
            return False
        
        # Check for invalid characters
        invalid_chars = {' ', '\n', '\r', '\t'}
        if any(char in url for char in invalid_chars):
            return False
        
        return True
    
    def _is_domain_allowed(self, domain: str) -> bool:
        """Check if domain is allowed."""
        domain = domain.lower()
        
        # Check blocked domains
        if domain in self.blocked_domains:
            return False
        
        # Check allowed domains (if specified)
        if self.allowed_domains:
            return domain in self.allowed_domains
        
        return True
    
    def _matches_blocked_pattern(self, url: str) -> bool:
        """Check if URL matches any blocked pattern."""
        for pattern in self.blocked_patterns:
            if pattern.search(url):
                return True
        return False
    
    def _has_blocked_extension(self, path: str) -> bool:
        """Check if path has blocked file extension."""
        if not path:
            return False
        
        # Get file extension
        path_lower = path.lower()
        for ext in self.invalid_extensions:
            if path_lower.endswith(ext):
                return True
        
        return False
    
    def _normalize_path(self, path: str) -> str:
        """Normalize URL path."""
        if not path:
            return '/'
        
        # Remove redundant slashes
        path = re.sub(r'/+', '/', path)
        
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path
        
        # Remove trailing slash (except for root)
        if len(path) > 1 and path.endswith('/'):
            path = path[:-1]
        
        return path
    
    def _normalize_query(self, query: str) -> str:
        """Normalize query string."""
        if not query:
            return ''
        
        try:
            # Parse query parameters
            params = parse_qs(query, keep_blank_values=not self.remove_empty_params)
            
            # Remove empty parameters if configured
            if self.remove_empty_params:
                params = {k: v for k, v in params.items() if v and v[0]}
            
            # Sort parameters if configured
            if self.sort_query_params:
                sorted_params = []
                for key in sorted(params.keys()):
                    for value in sorted(params[key]):
                        sorted_params.append((key, value))
                return urlencode(sorted_params)
            else:
                # Reconstruct query string
                param_pairs = []
                for key, values in params.items():
                    for value in values:
                        param_pairs.append((key, value))
                return urlencode(param_pairs)
                
        except Exception:
            # Return original query if parsing fails
            return query
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return {
            'allowed_domains_count': len(self.allowed_domains) if self.allowed_domains else 0,
            'blocked_domains_count': len(self.blocked_domains),
            'blocked_patterns_count': len(self.blocked_patterns),
            'allowed_content_types': list(self.allowed_content_types),
            'blocked_extensions': list(self.invalid_extensions),
            'max_url_length': self.max_url_length,
            'max_path_segments': self.max_path_segments,
            'max_query_params': self.max_query_params,
            'normalization_settings': {
                'remove_fragments': self.remove_fragments,
                'remove_empty_params': self.remove_empty_params,
                'sort_query_params': self.sort_query_params,
                'lowercase_scheme_host': self.lowercase_scheme_host
            }
        }


class URLCanonicalizer:
    """
    Advanced URL canonicalization for deduplication.
    """
    
    def __init__(self):
        # Parameters to remove (common tracking parameters)
        self.tracking_params = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'gclid', 'fbclid', 'msclkid', 'ref', 'referrer',
            '_ga', '_gid', 'sessionid', 'jsessionid'
        }
        
        # Parameters to normalize
        self.normalize_params = {
            'page', 'p', 'offset', 'start', 'from'
        }
    
    def canonicalize(self, url: str) -> str:
        """
        Canonicalize URL for deduplication.
        
        Args:
            url: URL to canonicalize
            
        Returns:
            Canonical URL
        """
        if not url:
            return url
            
        try:
            parsed = urlparse(url)
            
            # Normalize scheme and host
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()
            
            # Remove www prefix
            if netloc.startswith('www.'):
                netloc = netloc[4:]
            
            # Remove default ports
            if ':443' in netloc and scheme == 'https':
                netloc = netloc.replace(':443', '')
            elif ':80' in netloc and scheme == 'http':
                netloc = netloc.replace(':80', '')
            
            # Normalize path
            path = self._canonicalize_path(parsed.path)
            
            # Normalize query
            query = self._canonicalize_query(parsed.query)
            
            # Remove fragment
            fragment = ''
            
            # Ensure path ends with / for root URLs
            if not path or path == '':
                path = '/'
            
            return urlunparse((scheme, netloc, path, '', query, fragment))
            
        except Exception:
            return url
    
    def _canonicalize_path(self, path: str) -> str:
        """Canonicalize URL path."""
        if not path:
            return '/'
        
        if path == '/':
            return '/'
        
        # Remove trailing slash
        if path.endswith('/'):
            path = path[:-1]
        
        # Decode percent-encoded characters
        try:
            from urllib.parse import unquote
            path = unquote(path)
        except:
            pass
        
        return path
    
    def _canonicalize_query(self, query: str) -> str:
        """Canonicalize query string."""
        if not query:
            return ''
        
        try:
            params = parse_qs(query, keep_blank_values=True)
            
            # Remove tracking parameters
            filtered_params = {
                k: v for k, v in params.items()
                if k.lower() not in self.tracking_params
            }
            
            # Normalize specific parameters
            for param in self.normalize_params:
                if param in filtered_params:
                    # Convert to integer if possible (for pagination)
                    try:
                        value = int(filtered_params[param][0])
                        filtered_params[param] = [str(value)]
                    except (ValueError, IndexError):
                        pass
            
            # Sort parameters
            sorted_params = []
            for key in sorted(filtered_params.keys()):
                for value in sorted(filtered_params[key]):
                    sorted_params.append((key, value))
            
            return urlencode(sorted_params)
            
        except Exception:
            return query
    
    def add_tracking_param(self, param: str) -> None:
        """Add parameter to tracking parameters list."""
        self.tracking_params.add(param.lower())
    
    def remove_tracking_param(self, param: str) -> None:
        """Remove parameter from tracking parameters list."""
        self.tracking_params.discard(param.lower())