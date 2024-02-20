import os, sys
sys.path.append(os.path.abspath('./'))

import json
import logging
import requests
from dagster import asset, MetadataValue
from partitions import fantasy_partitions

