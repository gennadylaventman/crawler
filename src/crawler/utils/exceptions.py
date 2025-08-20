"""
Custom exceptions for the web crawler system.
"""


class CrawlerError(Exception):
    """Base exception for crawler-related errors."""
    pass


class NetworkError(CrawlerError):
    """Exception raised for network-related errors."""
    pass


class ContentError(CrawlerError):
    """Exception raised for content processing errors."""
    pass


class DatabaseError(CrawlerError):
    """Exception raised for database-related errors."""
    pass


class ConfigurationError(CrawlerError):
    """Exception raised for configuration-related errors."""
    pass


class ValidationError(CrawlerError):
    """Exception raised for data validation errors."""
    pass


class RateLimitError(CrawlerError):
    """Exception raised when rate limits are exceeded."""
    pass


class RobotsError(CrawlerError):
    """Exception raised for robots.txt related errors."""
    pass


class QueueError(CrawlerError):
    """Exception raised for URL queue related errors."""
    pass


class MetricsError(CrawlerError):
    """Exception raised for metrics collection errors."""
    pass


class LoggingError(CrawlerError):
    """Exception raised for logging configuration errors."""
    pass


class AnalyticsError(CrawlerError):
    """Exception raised for analytics processing errors."""
    pass