#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Matteo Romanello and Giovanni Colavizza

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler()) 