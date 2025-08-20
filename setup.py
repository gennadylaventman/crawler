"""
Setup configuration for the Web Crawler package.
"""

from setuptools import setup, find_packages
import os
import re

# Read version from __init__.py
def get_version():
    init_file = os.path.join(os.path.dirname(__file__), 'src', 'crawler', '__init__.py')
    with open(init_file, 'r', encoding='utf-8') as f:
        content = f.read()
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", content, re.M)
        if version_match:
            return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

# Read long description from README
def get_long_description():
    readme_file = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_file):
        with open(readme_file, 'r', encoding='utf-8') as f:
            return f.read()
    return "A professional web crawler with advanced features"

# Read requirements from requirements.txt
def get_requirements():
    requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_file):
        with open(requirements_file, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

# Development requirements
dev_requirements = [
    'pytest>=7.0.0',
    'pytest-asyncio>=0.21.0',
    'pytest-cov>=4.0.0',
    'black>=23.0.0',
    'flake8>=6.0.0',
    'mypy>=1.0.0',
    'isort>=5.12.0',
    'pre-commit>=3.0.0',
    'sphinx>=6.0.0',
    'sphinx-rtd-theme>=1.2.0',
]

# Testing requirements
test_requirements = [
    'pytest>=7.0.0',
    'pytest-asyncio>=0.21.0',
    'pytest-cov>=4.0.0',
    'pytest-mock>=3.10.0',
    'factory-boy>=3.2.0',
    'faker>=18.0.0',
]

# Documentation requirements
docs_requirements = [
    'sphinx>=6.0.0',
    'sphinx-rtd-theme>=1.2.0',
    'sphinx-autodoc-typehints>=1.22.0',
    'myst-parser>=1.0.0',
]

setup(
    name="webcrawler",
    version=get_version(),
    author="Your Name",
    author_email="your.email@company.com",
    description="A professional web crawler with advanced content analysis and monitoring",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourcompany/webcrawler",
    project_urls={
        "Bug Tracker": "https://github.com/yourcompany/webcrawler/issues",
        "Documentation": "https://webcrawler.readthedocs.io/",
        "Source Code": "https://github.com/yourcompany/webcrawler",
    },
    
    # Package configuration
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    
    # Python version requirement
    python_requires=">=3.9",
    
    # Dependencies
    install_requires=get_requirements(),
    
    # Optional dependencies
    extras_require={
        "dev": dev_requirements,
        "test": test_requirements,
        "docs": docs_requirements,
        "all": dev_requirements + test_requirements + docs_requirements,
    },
    
    # Entry points for CLI commands
    entry_points={
        "console_scripts": [
            "webcrawler=crawler.cli:main",
            "crawler=crawler.cli:main",
        ],
    },
    
    # Package data
    package_data={
        "crawler": [
            "config/*.yaml",
            "templates/*.html",
            "static/*",
        ],
    },
    
    # Classification
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Text Processing :: Indexing",
        "Topic :: Internet :: WWW/HTTP :: Site Management",
        "Framework :: AsyncIO",
    ],
    
    # Keywords
    keywords=[
        "web crawler", "web scraping", "content analysis", "text mining",
        "url management", "robots.txt", "sitemap", "async", "monitoring",
        "data extraction", "nlp", "word frequency", "content quality"
    ],
    
    # License
    license="MIT",
    
    # Zip safe
    zip_safe=False,
    
    # Additional metadata
    platforms=["any"],
    
    # Test suite
    test_suite="tests",
    tests_require=test_requirements,
    
    # Options for different tools
    options={
        "bdist_wheel": {
            "universal": False,
        },
    },
)