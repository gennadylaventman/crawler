"""
Content processing pipeline for text extraction and analysis.
"""

import re
import time
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
from collections import Counter
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Comment
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize

from crawler.utils.config import ContentConfig
from crawler.utils.exceptions import ContentError
from crawler.monitoring.metrics import PageMetrics


@dataclass
class ProcessedContent:
    """Result of content processing."""
    url: str
    title: Optional[str] = None
    meta_description: Optional[str] = None
    language: Optional[str] = None
    charset: Optional[str] = None
    
    # Text content
    raw_text: str = ""
    cleaned_text: str = ""
    word_count: int = 0
    unique_word_count: int = 0
    sentence_count: int = 0
    paragraph_count: int = 0
    
    # Word analysis
    word_frequencies: Optional[Dict[str, int]] = None
    average_word_length: float = 0.0
    
    # Links and structure
    links: Optional[List[str]] = None
    internal_links: Optional[List[str]] = None
    external_links: Optional[List[str]] = None
    
    # HTML structure metrics
    total_html_tags: int = 0
    image_count: int = 0
    form_count: int = 0
    script_count: int = 0
    style_count: int = 0
    
    # Quality metrics
    text_to_html_ratio: float = 0.0
    content_density: float = 0.0
    readability_score: Optional[float] = None
    
    def __post_init__(self):
        if self.word_frequencies is None:
            self.word_frequencies = {}
        if self.links is None:
            self.links = []
        if self.internal_links is None:
            self.internal_links = []
        if self.external_links is None:
            self.external_links = []


class ContentProcessor:
    """
    Content processing pipeline for extracting and analyzing web page content.
    """
    
    def __init__(self, config: ContentConfig):
        self.config = config
        self._stopwords: Optional[Set[str]] = None
        self._initialize_nltk()
    
    def _initialize_nltk(self) -> None:
        """Initialize NLTK resources."""
        try:
            # Download required NLTK data
            nltk.download('punkt', quiet=True)
            nltk.download('punkt_tab', quiet=True)  # New requirement for newer NLTK versions
            nltk.download('stopwords', quiet=True)
            
            # Load stopwords
            self._stopwords = set(stopwords.words('english'))
        except Exception as e:
            print(f"Warning: Failed to initialize NLTK: {e}")
            self._stopwords = set()
    
    async def process_content(self, html_content: str, url: str, metrics: PageMetrics) -> ProcessedContent:
        """
        Process HTML content and extract meaningful data.
        
        Args:
            html_content: Raw HTML content
            url: Source URL
            metrics: Metrics object to track timing
            
        Returns:
            ProcessedContent with extracted data
        """
        result = ProcessedContent(url=url)
        
        try:
            # Parse HTML
            start_time = time.perf_counter()
            soup = self._parse_html(html_content)
            metrics.html_parse_time = (time.perf_counter() - start_time) * 1000
            
            # Extract metadata
            await self._extract_metadata(soup, result)
            
            # Analyze HTML structure BEFORE removing scripts/styles
            await self._analyze_html_structure(soup, result)
            
            # Extract and clean text (this removes scripts/styles)
            start_time = time.perf_counter()
            result.raw_text = self._extract_text(soup)
            metrics.text_extraction_time = (time.perf_counter() - start_time) * 1000
            
            start_time = time.perf_counter()
            result.cleaned_text = self._clean_text(result.raw_text)
            metrics.text_cleaning_time = (time.perf_counter() - start_time) * 1000
            
            # Count sentences after text is cleaned
            await self._count_sentences(result)
            
            # Tokenize and analyze words
            start_time = time.perf_counter()
            words = self._tokenize_text(result.cleaned_text)
            metrics.word_tokenization_time = (time.perf_counter() - start_time) * 1000
            
            start_time = time.perf_counter()
            result.word_frequencies = self._count_words(words)
            result.word_count = len(words)
            result.unique_word_count = len(result.word_frequencies)
            result.average_word_length = sum(len(word) for word in words) / len(words) if words else 0
            metrics.word_counting_time = (time.perf_counter() - start_time) * 1000
            
            # Extract links
            start_time = time.perf_counter()
            await self._extract_links(soup, url, result)
            metrics.link_extraction_time = (time.perf_counter() - start_time) * 1000
            
            # Calculate quality metrics
            await self._calculate_quality_metrics(html_content, result)
                        
            return result
            
        except Exception as e:
            raise ContentError(f"Failed to process content: {e}")
    
    def _parse_html(self, html_content: str) -> BeautifulSoup:
        """Parse HTML content using BeautifulSoup."""
        try:
            # Use lxml parser for better performance
            soup = BeautifulSoup(html_content, 'lxml')
            return soup
        except Exception as e:
            # Fallback to html.parser
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                return soup
            except Exception as e2:
                raise ContentError(f"Failed to parse HTML: {e2}")
    
    async def _extract_metadata(self, soup: BeautifulSoup, result: ProcessedContent) -> None:
        """Extract metadata from HTML."""
        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            result.title = title_tag.get_text().strip()
        
        # Extract meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.name == 'meta':
            content = meta_desc.get('content') if hasattr(meta_desc, 'get') else None
            if content:
                result.meta_description = str(content).strip()
        
        # Extract charset
        charset_meta = soup.find('meta', attrs={'charset': True})
        if charset_meta and charset_meta.name == 'meta':
            charset = charset_meta.get('charset') if hasattr(charset_meta, 'get') else None
            if charset:
                result.charset = str(charset)
        else:
            # Try http-equiv content-type
            content_type_meta = soup.find('meta', attrs={'http-equiv': 'content-type'})
            if content_type_meta and content_type_meta.name == 'meta':
                content = content_type_meta.get('content') if hasattr(content_type_meta, 'get') else None
                if content:
                    content_str = str(content)
                    charset_match = re.search(r'charset=([^;]+)', content_str, re.IGNORECASE)
                    if charset_match:
                        result.charset = charset_match.group(1).strip()
    
    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract text content from HTML."""
        # Remove script and style elements if configured
        if self.config.remove_scripts:
            for script in soup(["script"]):
                script.decompose()
        
        if self.config.remove_styles:
            for style in soup(["style"]):
                style.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Extract text
        text = soup.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove non-printable characters
        text = re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', text)
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
        
        # Normalize whitespace again
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _tokenize_text(self, text: str) -> List[str]:
        """Tokenize text into words."""
        if not text:
            return []
        
        try:
            # Use NLTK word tokenizer if available
            words = word_tokenize(text.lower())
        except:
            # Fallback to simple regex tokenization
            words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        
        # Filter words
        filtered_words = []
        for word in words:
            # Skip short words
            if len(word) < 2:
                continue
            
            # Skip stopwords if configured
            if self._stopwords and word in self._stopwords:
                continue
            
            # Skip words that are too long (likely not real words)
            if len(word) > 50:
                continue
            
            # Skip words that contain numbers or symbols (additional filtering)
            if not word.isalpha():
                continue
            
            filtered_words.append(word)
        
        return filtered_words
    
    def _count_words(self, words: List[str]) -> Dict[str, int]:
        """Count word frequencies."""
        return dict(Counter(words))
    
    async def _extract_links(self, soup: BeautifulSoup, base_url: str, result: ProcessedContent) -> None:
        """Extract and categorize links."""
        links = []
        internal_links = []
        external_links = []
        
        base_domain = urlparse(base_url).netloc
        
        for link_tag in soup.find_all('a', href=True):
            href = link_tag.get('href')
            if not href:
                continue
            
            # Resolve relative URLs
            try:
                absolute_url = urljoin(base_url, href)
                links.append(absolute_url)
                
                # Categorize as internal or external
                link_domain = urlparse(absolute_url).netloc
                if link_domain == base_domain:
                    internal_links.append(absolute_url)
                else:
                    external_links.append(absolute_url)
                    
            except Exception:
                # Skip malformed URLs
                continue
        
        result.links = links
        result.internal_links = internal_links
        result.external_links = external_links
    
    async def _analyze_html_structure(self, soup: BeautifulSoup, result: ProcessedContent) -> None:
        """Analyze HTML structure and count elements."""
        # Count all HTML tags
        result.total_html_tags = len(soup.find_all())
        
        # Count specific elements
        result.image_count = len(soup.find_all('img'))
        result.form_count = len(soup.find_all('form'))
        result.script_count = len(soup.find_all('script'))
        result.style_count = len(soup.find_all('style'))
        
        # Count paragraphs
        result.paragraph_count = len(soup.find_all('p'))
        
        # Note: sentence_count will be calculated later after text is cleaned
    
    async def _calculate_quality_metrics(self, html_content: str, result: ProcessedContent) -> None:
        """Calculate content quality metrics."""
        html_length = len(html_content)
        text_length = len(result.cleaned_text)
        
        # Text to HTML ratio
        if html_length > 0:
            result.text_to_html_ratio = text_length / html_length
        
        # Content density (text per HTML tag)
        if result.total_html_tags > 0:
            result.content_density = text_length / result.total_html_tags
        
    async def _count_sentences(self, result: ProcessedContent) -> None:
        """Count sentences in the cleaned text."""
        if not result.cleaned_text:
            result.sentence_count = 0
            return
        
        try:
            result.sentence_count = len(sent_tokenize(result.cleaned_text))
        except Exception as e:
            # Fallback to simple sentence counting
            result.sentence_count = len(re.findall(r'[.!?]+', result.cleaned_text))
            print(f"NLTK sentence tokenization failed, using fallback: {e}")
    
    def is_content_valid(self, content: ProcessedContent) -> bool:
        """Check if processed content meets minimum quality requirements."""
        # Check minimum text length
        if len(content.cleaned_text) < self.config.min_text_length:
            return False
        
        # Check maximum words per page
        if content.word_count > self.config.max_words_per_page:
            return False
        
        # Check if we have meaningful content
        if content.word_count < 10:
            return False
        
        return True
    
    def get_content_summary(self, content: ProcessedContent) -> Dict[str, Any]:
        """Get a summary of processed content."""
        return {
            'url': content.url,
            'title': content.title,
            'language': content.language,
            'word_count': content.word_count,
            'unique_words': content.unique_word_count,
            'sentence_count': content.sentence_count,
            'paragraph_count': content.paragraph_count,
            'link_count': len(content.links) if content.links else 0,
            'internal_links': len(content.internal_links) if content.internal_links else 0,
            'external_links': len(content.external_links) if content.external_links else 0,
            'text_to_html_ratio': content.text_to_html_ratio,
            'readability_score': content.readability_score,
            'top_words': dict(list(sorted(content.word_frequencies.items(),
                                        key=lambda x: x[1], reverse=True))[:10]) if content.word_frequencies else {}
        }