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
    
    def calculate_tf_idf(self, documents: List[str], include_stopwords: bool = False) -> Dict[str, Dict[str, float]]:
        """
        Calculate TF-IDF scores for multiple documents.
        
        Args:
            documents: List of document texts
            include_stopwords: Whether to include stop words
            
        Returns:
            Dictionary mapping document index to word TF-IDF scores
        """
        try:
            if not documents:
                return {}
            
            # Extract words from all documents
            doc_words = []
            all_words = set()
            
            for doc in documents:
                words = self._extract_words(doc, include_stopwords)
                doc_words.append(words)
                all_words.update(words)
            
            # Calculate document frequency for each word
            doc_freq = {}
            for word in all_words:
                doc_freq[word] = sum(1 for words in doc_words if word in words)
            
            # Calculate TF-IDF for each document
            tf_idf_results = {}
            
            for doc_idx, words in enumerate(doc_words):
                word_counts = Counter(words)
                doc_length = len(words)
                tf_idf_scores = {}
                
                for word in word_counts:
                    # Term frequency
                    tf = word_counts[word] / doc_length
                    
                    # Inverse document frequency
                    idf = math.log(len(documents) / doc_freq[word])
                    
                    # TF-IDF score
                    tf_idf_scores[word] = tf * idf
                
                tf_idf_results[doc_idx] = tf_idf_scores
            
            return tf_idf_results
            
        except Exception as e:
            raise ContentError(f"Failed to calculate TF-IDF: {e}")
    
    def find_keywords(self, text: str, num_keywords: int = 10) -> List[Tuple[str, float]]:
        """
        Extract keywords using frequency and length-based scoring.
        
        Args:
            text: Text to extract keywords from
            num_keywords: Number of keywords to return
            
        Returns:
            List of (keyword, score) tuples
        """
        try:
            words = self._extract_words(text, include_stopwords=False)
            
            if not words:
                return []
            
            word_counts = Counter(words)
            total_words = len(words)
            
            # Calculate keyword scores
            keyword_scores = {}
            
            for word, count in word_counts.items():
                # Base score from frequency
                frequency_score = count / total_words
                
                # Length bonus (longer words often more meaningful)
                length_bonus = min(len(word) / 10, 1.0)
                
                # Rarity bonus (less common words more important)
                rarity_bonus = 1.0 / math.log(count + 1)
                
                # Combined score
                keyword_scores[word] = frequency_score * (1 + length_bonus + rarity_bonus)
            
            # Sort by score and return top keywords
            sorted_keywords = sorted(keyword_scores.items(), key=lambda x: x[1], reverse=True)
            
            return sorted_keywords[:num_keywords]
            
        except Exception as e:
            raise ContentError(f"Failed to find keywords: {e}")
    
    def compare_texts(self, text1: str, text2: str) -> Dict[str, float]:
        """
        Compare two texts using various similarity metrics.
        
        Args:
            text1: First text
            text2: Second text
            
        Returns:
            Dictionary of similarity metrics
        """
        try:
            words1 = set(self._extract_words(text1, include_stopwords=False))
            words2 = set(self._extract_words(text2, include_stopwords=False))
            
            if not words1 or not words2:
                return {'jaccard': 0.0, 'overlap': 0.0, 'dice': 0.0}
            
            # Jaccard similarity
            intersection = len(words1.intersection(words2))
            union = len(words1.union(words2))
            jaccard = intersection / union if union > 0 else 0.0
            
            # Overlap coefficient
            overlap = intersection / min(len(words1), len(words2))
            
            # Dice coefficient
            dice = (2 * intersection) / (len(words1) + len(words2))
            
            return {
                'jaccard': jaccard,
                'overlap': overlap,
                'dice': dice,
                'common_words': intersection,
                'unique_words1': len(words1 - words2),
                'unique_words2': len(words2 - words1)
            }
            
        except Exception as e:
            raise ContentError(f"Failed to compare texts: {e}")
    
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