#!/bin/bash

/bin/date

source /home/mromanello/.pyenv/versions/lb-refactored/bin/activate
which python
cd ~/Documents/LinkedBooks/linkedbooks_refactored/api/

#echo "Clearing the Redis cache"
#/usr/local/bin/redis-cli flushall
#echo "Keyspace after clearing:"
#/usr/local/bin/redis-cli info|grep keys=

python api_dev_pre_caching.py
