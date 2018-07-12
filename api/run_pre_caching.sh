#!/bin/bash

/bin/date

source /home/mromanello/.pyenv/versions/lb-refactored/bin/activate
which python
cd ~/Documents/LinkedBooks/linkedbooks_refactored/api/


python api_pre_caching.py
