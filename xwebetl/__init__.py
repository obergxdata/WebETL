"""
xwebetl - A flexible web content extraction, transformation, and loading (ETL) pipeline.

This package provides tools for navigating websites, extracting data,
transforming it with LLMs, and generating structured output.
"""

from xwebetl.extract.dispatch import Dispatcher
from xwebetl.transform.transform import Transform
from xwebetl.load.load import Load
from xwebetl.source.source_manager import Source

__all__ = ['Dispatcher', 'Transform', 'Load', 'Source']
__version__ = '0.1.0'
