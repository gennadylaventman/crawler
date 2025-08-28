"""
Content extraction from HTML pages.
"""

import re
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Comment

from crawler.utils.exceptions import ContentError


from crawler.utils.logging import get_logger


logger = get_logger('extractor')


class ContentExtractor:
    """
    HTML content extractor for text, metadata, and links.
    """
    
    def __init__(self):
        # Elements to remove
        self.remove_elements = {
            'script', 'style', 'noscript', 'iframe', 'object', 'embed',
            'form', 'input', 'button', 'select', 'textarea'
        }
        
        # Elements that typically contain navigation/boilerplate
        self.navigation_elements = {
            'nav', 'header', 'footer', 'aside', 'menu'
        }
        
        # Block-level elements for text extraction
        self.block_elements = {
            'div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'article', 'section', 'main', 'blockquote', 'pre'
        }
    
    async def extract_text(self, html: str, remove_navigation: bool = True) -> str:
        """
        Extract clean text from HTML.
        
        Args:
            html: HTML content
            remove_navigation: Whether to remove navigation elements
            
        Returns:
            Extracted text
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Remove unwanted elements
            self._remove_unwanted_elements(soup)
            
            # Remove navigation if requested
            if remove_navigation:
                self._remove_navigation_elements(soup)
            
            # Extract text
            text = self._extract_text_from_soup(soup, remove_navigation)
            
            # Clean and normalize text
            text = self._clean_text(text)
            
            return text
            
        except Exception as e:
            raise ContentError(f"Failed to extract text: {e}")
    
    async def extract_metadata(self, html: str) -> Dict[str, str]:
        """
        Extract metadata from HTML.
        
        Args:
            html: HTML content
            
        Returns:
            Dictionary of metadata
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            metadata = {}
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag and title_tag.string:
                metadata['title'] = title_tag.string.strip()
            
            # Extract meta tags
            meta_tags = soup.find_all('meta')
            for meta in meta_tags:
                # 🐛 FIX: Add proper null and type checks for meta elements
                if meta is None or not hasattr(meta, 'get'):
                    continue
                
                try:
                    name = meta.get('name') or meta.get('property') or meta.get('http-equiv')
                    content = meta.get('content')
                    
                    if name and content:
                        # Ensure we have strings before calling methods
                        name_str = str(name) if name else ''
                        content_str = str(content) if content else ''
                        if name_str and content_str:
                            metadata[name_str.lower()] = content_str.strip()
                except (AttributeError, TypeError) as e:
                    # 🐛 DEBUG: Log extraction errors for diagnosis
                    logger.debug(f"Error extracting meta tag: {e}, meta: {meta}")
                    continue
            
            # Extract specific metadata
            self._extract_structured_metadata(soup, metadata)
            
            return metadata
            
        except Exception as e:
            raise ContentError(f"Failed to extract metadata: {e}")
    
    async def extract_links(self, html: str, base_url: str) -> List[str]:
        """
        Extract and resolve links from HTML.
        
        Args:
            html: HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            links = []
            
            # Extract links from anchor tags
            for link_tag in soup.find_all('a', href=True):
                # 🐛 FIX: Add null checks for link elements
                if link_tag is None or not hasattr(link_tag, 'get'):
                    continue
                    
                try:
                    href = link_tag.get('href')
                    if href:
                        href_str = str(href) if href else ''
                        if href_str:
                            absolute_url = self._resolve_url(href_str, base_url)
                            if absolute_url and self._is_valid_link(absolute_url):
                                links.append(absolute_url)
                except (AttributeError, TypeError):
                    continue
            
            # Extract links from other elements
            for img_tag in soup.find_all('img', src=True):
                # 🐛 FIX: Add null checks for img elements
                if img_tag is None or not hasattr(img_tag, 'get'):
                    continue
                    
                try:
                    src = img_tag.get('src')
                    if src:
                        src_str = str(src) if src else ''
                        if src_str:
                            absolute_url = self._resolve_url(src_str, base_url)
                            if absolute_url and self._is_valid_link(absolute_url):
                                links.append(absolute_url)
                except (AttributeError, TypeError):
                    continue
            
            # Remove duplicates while preserving order
            unique_links = []
            seen = set()
            for link in links:
                if link not in seen:
                    unique_links.append(link)
                    seen.add(link)
            
            return unique_links
            
        except Exception as e:
            raise ContentError(f"Failed to extract links: {e}")
    
    async def clean_text(self, text: str) -> str:
        """
        Clean and normalize extracted text.
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text
        """
        return self._clean_text(text)
    
    def _remove_unwanted_elements(self, soup: BeautifulSoup) -> None:
        """Remove unwanted HTML elements."""
        # Remove script and style elements
        for element_name in self.remove_elements:
            for element in soup.find_all(element_name):
                element.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Remove elements with display:none or visibility:hidden
        from bs4.element import Tag
        
        for element in soup.find_all(style=True):
            if isinstance(element, Tag):
                # 🐛 DEBUG: Add null check and logging for diagnosis
                if element is None:
                    logger.warning("Found None element in soup.find_all(style=True) - skipping")
                    continue
                    
                try:
                    style_attr = element.get('style', '')
                    style = str(style_attr).lower() if style_attr is not None else ''
                    if 'display:none' in style or 'visibility:hidden' in style:
                        element.decompose()
                except AttributeError as e:
                    # 🐛 DEBUG: Log the specific AttributeError for diagnosis
                    logger.warning(
                        f"AttributeError in _remove_unwanted_elements: {e}, "
                        f"element type: {type(element)}, element: {element}"
                    )
                    continue
            else:
                # 🐛 DEBUG: Log non-Tag elements for diagnosis
                logger.debug(f"Non-Tag element found: {type(element)}")
    
    def _remove_navigation_elements(self, soup: BeautifulSoup) -> None:
        """Remove navigation and boilerplate elements."""
        for element_name in self.navigation_elements:
            for element in soup.find_all(element_name):
                element.decompose()
        
        # Remove elements with navigation-related classes/ids
        nav_patterns = [
            'nav', 'menu', 'sidebar', 'header', 'footer',
            'breadcrumb', 'pagination', 'social', 'share'
        ]
        
        for pattern in nav_patterns:
            # Remove by class
            for element in soup.find_all(class_=re.compile(pattern, re.I)):
                element.decompose()
            
            # Remove by id
            for element in soup.find_all(id=re.compile(pattern, re.I)):
                element.decompose()
    
    def _extract_text_from_soup(self, soup: BeautifulSoup, remove_navigation: bool = True) -> str:
        """Extract text from BeautifulSoup object."""
        # If we're keeping navigation, extract from entire document
        if not remove_navigation:
            text = soup.get_text(separator=' ', strip=True)
        else:
            # Try to find main content area first
            main_content = self._find_main_content(soup)
            
            if main_content:
                text = main_content.get_text(separator=' ', strip=True)
            else:
                text = soup.get_text(separator=' ', strip=True)
        
        return text
    
    def _find_main_content(self, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """Find main content area in HTML."""
        # Look for semantic HTML5 elements
        main_selectors = [
            'main',
            'article',
            '[role="main"]',
            '.main-content',
            '.content',
            '.post-content',
            '.entry-content',
            '#main',
            '#content'
        ]
        
        for selector in main_selectors:
            element = soup.select_one(selector)
            if element:
                return element
        
        # Look for largest text block
        text_blocks = []
        for element in soup.find_all(self.block_elements):
            text = element.get_text(strip=True)
            if len(text) > 50:  # Minimum text length
                text_blocks.append((element, len(text)))
        
        if text_blocks:
            # Return element with most text
            text_blocks.sort(key=lambda x: x[1], reverse=True)
            return text_blocks[0][0]
        
        return None
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove excessive punctuation
        text = re.sub(r'[.]{3,}', '...', text)
        text = re.sub(r'[-]{3,}', '---', text)
        
        # Remove non-printable characters except common ones
        text = re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', text)
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
        
        # Clean up extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _resolve_url(self, url: str, base_url: str) -> Optional[str]:
        """Resolve relative URL to absolute URL."""
        try:
            if not url or not base_url:
                return None
            
            # Skip non-HTTP URLs
            if url.startswith(('mailto:', 'tel:', 'javascript:', 'data:')):
                return None
            
            # Resolve relative URL
            absolute_url = urljoin(base_url, url)
            
            # Validate URL
            parsed = urlparse(absolute_url)
            if not parsed.scheme or not parsed.netloc:
                return None
            
            return absolute_url
            
        except Exception:
            return None
    
    def _is_valid_link(self, url: str) -> bool:
        """Check if link is valid for crawling."""
        try:
            parsed = urlparse(url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Must be HTTP/HTTPS
            if parsed.scheme.lower() not in ('http', 'https'):
                return False
            
            # Skip common file extensions
            path = parsed.path.lower()
            skip_extensions = {
                '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.zip', '.rar', '.tar', '.gz', '.7z',
                '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
                '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv',
                '.exe', '.msi', '.dmg', '.deb', '.rpm'
            }
            
            for ext in skip_extensions:
                if path.endswith(ext):
                    return False
            
            return True
            
        except Exception:
            return False
    
    def _extract_structured_metadata(self, soup: BeautifulSoup, metadata: Dict[str, str]) -> None:
        """Extract structured metadata (JSON-LD, microdata, etc.)."""
        # Extract JSON-LD
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            script_text = script.get_text()
            if script_text:
                try:
                    import json
                    data = json.loads(script_text)
                    if isinstance(data, dict):
                        # Extract common fields
                        if 'name' in data:
                            metadata['structured_name'] = str(data['name'])
                        if 'description' in data:
                            metadata['structured_description'] = str(data['description'])
                        if '@type' in data:
                            metadata['structured_type'] = str(data['@type'])
                except (json.JSONDecodeError, Exception):
                    continue
        
        # Extract Open Graph metadata
        og_tags = soup.find_all('meta', property=re.compile(r'^og:'))
        for tag in og_tags:
            if hasattr(tag, 'get'):
                prop = tag.get('property')
                content = tag.get('content')
                if prop and content:
                    metadata[prop] = content.strip()
        
        # Extract Twitter Card metadata
        twitter_tags = soup.find_all('meta', attrs={'name': re.compile(r'^twitter:')})
        for tag in twitter_tags:
            if hasattr(tag, 'get'):
                name = tag.get('name')
                content = tag.get('content')
                if name and content:
                    metadata[name] = content.strip()
    
    def _normalize_path(self, path: str) -> str:
        """Normalize URL path."""
        if not path:
            return "/"
        
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path
        
        # Remove multiple slashes
        path = re.sub(r'/+', '/', path)
        
        # Remove trailing slash except for root
        if len(path) > 1 and path.endswith('/'):
            path = path[:-1]
        
        return path
    
    def _normalize_query(self, query: str) -> str:
        """Normalize URL query string."""
        if not query:
            return ""
        
        # Parse query parameters
        from urllib.parse import parse_qsl, urlencode
        
        try:
            params = parse_qsl(query, keep_blank_values=not getattr(self, 'remove_empty_params', False))
            
            # Remove empty parameters if configured
            if getattr(self, 'remove_empty_params', False):
                params = [(k, v) for k, v in params if v]
            
            # Sort parameters if configured
            if getattr(self, 'sort_query_params', False):
                params.sort()
            
            return urlencode(params)
        except Exception:
            return query