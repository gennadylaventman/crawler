"""
Word frequency analysis and content analytics.
"""

import re
import math
from typing import Dict, List, Tuple, Set, Any, Optional
from collections import Counter, defaultdict
from dataclasses import dataclass

from crawler.utils.exceptions import ContentError


@dataclass
class WordAnalysis:
    """Results of word frequency analysis."""
    word_frequencies: Dict[str, int]
    total_words: int
    unique_words: int
    average_word_length: float
    top_words: List[Tuple[str, int]]
    word_length_distribution: Dict[int, int]
    stopword_count: int
    rare_words: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'word_frequencies': self.word_frequencies,
            'total_words': self.total_words,
            'unique_words': self.unique_words,
            'average_word_length': self.average_word_length,
            'top_words': self.top_words,
            'word_length_distribution': self.word_length_distribution,
            'stopword_count': self.stopword_count,
            'rare_words': self.rare_words
        }


class WordFrequencyAnalyzer:
    """
    Advanced word frequency analysis with statistical features.
    """
    
    def __init__(self):
        # Common English stop words
        self.stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'the', 'this', 'but', 'they', 'have',
            'had', 'what', 'said', 'each', 'which', 'she', 'do', 'how', 'their',
            'if', 'up', 'out', 'many', 'then', 'them', 'these', 'so', 'some',
            'her', 'would', 'make', 'like', 'into', 'him', 'time', 'two', 'more',
            'go', 'no', 'way', 'could', 'my', 'than', 'first', 'been', 'call',
            'who', 'oil', 'sit', 'now', 'find', 'down', 'day', 'did', 'get',
            'come', 'made', 'may', 'part'
        }
        
        # Word patterns
        self.word_pattern = re.compile(r'\b[a-zA-Z]+\b')
        self.sentence_pattern = re.compile(r'[.!?]+')
        
        # Analysis settings
        self.min_word_length = 2
        self.max_word_length = 50
        self.rare_word_threshold = 1  # Words appearing only once
        self.top_words_limit = 50
    
    def analyze_text(self, text: str, include_stopwords: bool = False) -> WordAnalysis:
        """
        Perform comprehensive word frequency analysis.
        
        Args:
            text: Text to analyze
            include_stopwords: Whether to include stop words in analysis
            
        Returns:
            WordAnalysis results
        """
        try:
            if not text:
                return self._empty_analysis()
            
            # Extract words
            words = self._extract_words(text, include_stopwords)
            
            if not words:
                return self._empty_analysis()
            
            # Calculate frequencies
            word_frequencies = dict(Counter(words))
            
            # Calculate statistics
            total_words = len(words)
            unique_words = len(word_frequencies)
            average_word_length = sum(len(word) for word in words) / total_words
            
            # Get top words
            top_words = Counter(word_frequencies).most_common(self.top_words_limit)
            
            # Word length distribution
            word_length_dist = defaultdict(int)
            for word in words:
                word_length_dist[len(word)] += 1
            
            # Count stopwords
            stopword_count = sum(1 for word in words if word.lower() in self.stop_words)
            
            # Find rare words
            rare_words = [word for word, count in word_frequencies.items() 
                         if count <= self.rare_word_threshold]
            
            return WordAnalysis(
                word_frequencies=word_frequencies,
                total_words=total_words,
                unique_words=unique_words,
                average_word_length=average_word_length,
                top_words=top_words,
                word_length_distribution=dict(word_length_dist),
                stopword_count=stopword_count,
                rare_words=rare_words[:100]  # Limit rare words list
            )
            
        except Exception as e:
            raise ContentError(f"Failed to analyze text: {e}")
        
    def _extract_words(self, text: str, include_stopwords: bool = False) -> List[str]:
        """Extract and filter words from text."""
        if not text:
            return []
        
        # Find all words
        words = self.word_pattern.findall(text.lower())
        
        # Filter words
        filtered_words = []
        for word in words:
            # Check length
            if len(word) < self.min_word_length or len(word) > self.max_word_length:
                continue
            
            # Check stopwords
            if not include_stopwords and word in self.stop_words:
                continue
            
            # Check if word is alphabetic
            if not word.isalpha():
                continue
            
            filtered_words.append(word)
        
        return filtered_words
    
    def _empty_analysis(self) -> WordAnalysis:
        """Return empty analysis results."""
        return WordAnalysis(
            word_frequencies={},
            total_words=0,
            unique_words=0,
            average_word_length=0.0,
            top_words=[],
            word_length_distribution={},
            stopword_count=0,
            rare_words=[]
        )
    
    def get_word_statistics(self, word_frequencies: Dict[str, int]) -> Dict[str, Any]:
        """Get detailed statistics about word frequencies."""
        if not word_frequencies:
            return {}
        
        frequencies = list(word_frequencies.values())
        words = list(word_frequencies.keys())
        
        return {
            'total_unique_words': len(words),
            'total_word_occurrences': sum(frequencies),
            'max_frequency': max(frequencies),
            'min_frequency': min(frequencies),
            'average_frequency': sum(frequencies) / len(frequencies),
            'median_frequency': sorted(frequencies)[len(frequencies) // 2],
            'words_appearing_once': sum(1 for f in frequencies if f == 1),
            'most_frequent_word': max(word_frequencies.items(), key=lambda x: x[1]),
            'longest_word': max(words, key=len),
            'shortest_word': min(words, key=len),
            'average_word_length': sum(len(word) for word in words) / len(words)
        }