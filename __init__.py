"""
HT CHPP - Clean Python library for Hattrick API access with structured data processing.
"""

from .client import HTTPClient
from .config import Config
from .processors.generic import GenericProcessor

__version__ = "2.0.0"
__all__ = ["HTTPClient", "Config", "GenericProcessor"]