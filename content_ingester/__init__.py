#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Matteo Romanello, matteo.romanello@epfl.ch

from .content_ingestion import *
# Set default logging handler to avoid "No handler found" warnings.
from .version import __version__
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler()) 

__all__=[
	"__version__"
	,
]