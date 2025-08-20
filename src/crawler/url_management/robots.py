"""
Robots.txt compliance and sitemap discovery for the web crawler.
"""

import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import aiohttp


class RobotsChecker:
    """
    Robots.txt compliance checker with caching and sitemap discovery.
    """
    
    def __init__(self, user_agent: str = "*", cache_ttl: int = 3600):
        self.user_agent = user_agent
        self.cache_ttl = cache_ttl
        
        # Cache for robots.txt files
        self._robots_cache: Dict[str, Tuple[Optional[RobotFileParser], float]] = {}
        self._sitemap_cache: Dict[str, Tuple[List[str], float]] = {}
        
        # Rate limiting per domain
        self._crawl_delays: Dict[str, float] = {}
        self._last_access: Dict[str, float] = {}
        
        # Request session for fetching robots.txt
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()
    
    async def _create_session(self) -> None:
        """Create HTTP session for robots.txt requests."""
        if not self._session:
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={'User-Agent': f'WebCrawler/1.0 ({self.user_agent})'}
            )
    
    async def _close_session(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def can_fetch(self, url: str, user_agent: Optional[str] = None) -> bool:
        """
        Check if URL can be fetched according to robots.txt.
        
        Args:
            url: URL to check
            user_agent: User agent to check for (defaults to instance user_agent)
            
        Returns:
            True if URL can be fetched, False otherwise
        """
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                return False
            
            # Get robots.txt for domain
            robots_parser = await self._get_robots_parser(domain)
            
            if not robots_parser:
                # If no robots.txt or error fetching, allow crawling
                return True
            
            # Check if URL can be fetched
            check_user_agent = user_agent or self.user_agent
            return robots_parser.can_fetch(check_user_agent, url)
            
        except Exception:
            # On error, allow crawling (fail open)
            return True
    
    async def get_crawl_delay(self, url: str, user_agent: Optional[str] = None) -> float:
        """
        Get crawl delay for domain from robots.txt.
        
        Args:
            url: URL to get delay for
            user_agent: User agent to check for
            
        Returns:
            Crawl delay in seconds (0 if not specified)
        """
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                return 0.0
            
            # Check cache first
            if domain in self._crawl_delays:
                return self._crawl_delays[domain]
            
            # Get robots.txt for domain
            robots_parser = await self._get_robots_parser(domain)
            
            if not robots_parser:
                self._crawl_delays[domain] = 0.0
                return 0.0
            
            # Get crawl delay
            check_user_agent = user_agent or self.user_agent
            delay = robots_parser.crawl_delay(check_user_agent)
            
            # Default to 0 if not specified
            if delay is None:
                delay = 0.0
            else:
                # Ensure delay is float
                delay = float(delay)
            
            self._crawl_delays[domain] = delay
            return delay
            
        except Exception:
            return 0.0
    
    async def get_sitemaps(self, url: str) -> List[str]:
        """
        Get sitemap URLs from robots.txt.
        
        Args:
            url: URL to get sitemaps for
            
        Returns:
            List of sitemap URLs
        """
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                return []
            
            # Check cache first
            current_time = time.time()
            if domain in self._sitemap_cache:
                sitemaps, cached_time = self._sitemap_cache[domain]
                if current_time - cached_time < self.cache_ttl:
                    return sitemaps
            
            # Get robots.txt for domain
            robots_parser = await self._get_robots_parser(domain)
            
            if not robots_parser:
                return []
            
            # Get sitemaps
            sitemaps = robots_parser.site_maps() if hasattr(robots_parser, 'site_maps') else []
            if sitemaps is None:
                sitemaps = []
            
            # Cache results
            self._sitemap_cache[domain] = (sitemaps, current_time)
            
            return sitemaps
            
        except Exception:
            return []
    
    async def should_wait_for_crawl_delay(self, url: str, user_agent: Optional[str] = None) -> float:
        """
        Check if we should wait before crawling and return wait time.
        
        Args:
            url: URL to check
            user_agent: User agent to check for
            
        Returns:
            Time to wait in seconds (0 if no wait needed)
        """
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                return 0.0
            
            # Get crawl delay
            crawl_delay = await self.get_crawl_delay(url, user_agent)
            
            if crawl_delay <= 0:
                return 0.0
            
            # Check last access time
            current_time = time.time()
            last_access = self._last_access.get(domain, 0)
            time_since_last = current_time - last_access
            
            if time_since_last >= crawl_delay:
                # Update last access time
                self._last_access[domain] = current_time
                return 0.0
            else:
                # Need to wait
                wait_time = crawl_delay - time_since_last
                return wait_time
                
        except Exception:
            return 0.0
    
    async def _get_robots_parser(self, domain: str) -> Optional[RobotFileParser]:
        """
        Get robots.txt parser for domain with caching.
        
        Args:
            domain: Domain to get robots.txt for
            
        Returns:
            RobotFileParser instance or None if not available
        """
        current_time = time.time()
        
        # Check cache first
        if domain in self._robots_cache:
            parser, cached_time = self._robots_cache[domain]
            if current_time - cached_time < self.cache_ttl:
                return parser
        
        # Fetch robots.txt
        robots_url = f"https://{domain}/robots.txt"
        
        try:
            # Ensure session exists
            if not self._session:
                await self._create_session()
            
            async with self._session.get(robots_url) as response:
                if response.status == 200:
                    robots_content = await response.text()
                    
                    # Create parser and parse content
                    parser = RobotFileParser()
                    parser.set_url(robots_url)
                    
                    # Parse content using the parse method with lines
                    lines = robots_content.splitlines()
                    parser.parse(lines)
                    
                    # Cache parser
                    self._robots_cache[domain] = (parser, current_time)
                    
                    return parser
                else:
                    # Cache empty result for failed requests
                    self._robots_cache[domain] = (None, current_time)
                    return None
                    
        except Exception as e:
            # Cache empty result for errors
            self._robots_cache[domain] = (None, current_time)
            return None
    
    def clear_cache(self, domain: Optional[str] = None) -> None:
        """
        Clear robots.txt cache.
        
        Args:
            domain: Specific domain to clear (None for all)
        """
        if domain:
            self._robots_cache.pop(domain, None)
            self._sitemap_cache.pop(domain, None)
            self._crawl_delays.pop(domain, None)
            self._last_access.pop(domain, None)
        else:
            self._robots_cache.clear()
            self._sitemap_cache.clear()
            self._crawl_delays.clear()
            self._last_access.clear()
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            'robots_cached': len(self._robots_cache),
            'sitemaps_cached': len(self._sitemap_cache),
            'crawl_delays_cached': len(self._crawl_delays),
            'domains_tracked': len(self._last_access)
        }


class SitemapParser:
    """
    Sitemap parser for discovering URLs.
    """
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()
    
    async def _create_session(self) -> None:
        """Create HTTP session."""
        if not self._session:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={'User-Agent': 'WebCrawler/1.0 SitemapParser'}
            )
    
    async def _close_session(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def parse_sitemap(self, sitemap_url: str, max_urls: int = 10000) -> List[str]:
        """
        Parse sitemap and extract URLs.
        
        Args:
            sitemap_url: URL of sitemap to parse
            max_urls: Maximum number of URLs to return
            
        Returns:
            List of URLs found in sitemap
        """
        try:
            # Ensure session exists
            if not self._session:
                await self._create_session()
            
            async with self._session.get(sitemap_url) as response:
                if response.status != 200:
                    return []
                
                content = await response.text()
                
                # Parse XML content
                urls = self._extract_urls_from_xml(content, max_urls)
                
                return urls
                
        except Exception:
            return []
    
    def _extract_urls_from_xml(self, xml_content: str, max_urls: int) -> List[str]:
        """
        Extract URLs from sitemap XML content.
        
        Args:
            xml_content: XML content of sitemap
            max_urls: Maximum URLs to extract
            
        Returns:
            List of URLs
        """
        urls = []
        
        try:
            import xml.etree.ElementTree as ET
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Handle different sitemap formats
            namespaces = {
                'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9',
                'image': 'http://www.google.com/schemas/sitemap-image/1.1',
                'video': 'http://www.google.com/schemas/sitemap-video/1.1'
            }
            
            # Look for URL elements
            for url_elem in root.findall('.//sitemap:url', namespaces):
                loc_elem = url_elem.find('sitemap:loc', namespaces)
                if loc_elem is not None and loc_elem.text:
                    urls.append(loc_elem.text.strip())
                    
                    if len(urls) >= max_urls:
                        break
            
            # If no URLs found with namespace, try without
            if not urls:
                for url_elem in root.findall('.//url'):
                    loc_elem = url_elem.find('loc')
                    if loc_elem is not None and loc_elem.text:
                        urls.append(loc_elem.text.strip())
                        
                        if len(urls) >= max_urls:
                            break
            
            # Check for sitemap index
            if not urls:
                for sitemap_elem in root.findall('.//sitemap:sitemap', namespaces):
                    loc_elem = sitemap_elem.find('sitemap:loc', namespaces)
                    if loc_elem is not None and loc_elem.text:
                        # This is a sitemap index, would need recursive parsing
                        # For now, just return the sitemap URL
                        urls.append(loc_elem.text.strip())
                        
                        if len(urls) >= max_urls:
                            break
            
        except Exception:
            # Fallback: simple regex extraction
            import re
            url_pattern = r'<loc>(.*?)</loc>'
            matches = re.findall(url_pattern, xml_content, re.IGNORECASE)
            urls = [match.strip() for match in matches[:max_urls]]
        
        return urls
    
    async def discover_sitemaps(self, base_url: str) -> List[str]:
        """
        Discover sitemap URLs for a domain.
        
        Args:
            base_url: Base URL to discover sitemaps for
            
        Returns:
            List of discovered sitemap URLs
        """
        parsed_url = urlparse(base_url)
        base_domain_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        # Common sitemap locations
        common_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemaps.xml',
            '/sitemap/sitemap.xml',
            '/sitemaps/sitemap.xml'
        ]
        
        discovered_sitemaps = []
        
        for path in common_paths:
            sitemap_url = base_domain_url + path
            
            try:
                # Ensure session exists
                if not self._session:
                    await self._create_session()
                
                async with self._session.head(sitemap_url) as response:
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '').lower()
                        if 'xml' in content_type:
                            discovered_sitemaps.append(sitemap_url)
                            
            except Exception:
                continue
        
        return discovered_sitemaps