'''Go through a response object containing driver fantasy results and prices, save into parquet'''

import os, sys
sys.path.append(os.path.abspath('./'))

from src.utils.iomanager import polars_to_parquet
from src.raw_operations.fantasy.fantasy_results_parser import parse_results

'''Received schema:
[{}] response: list of 20 dictionaries (one per driver) [ {}, {} ]
    abbreviation: str,
    color: str,
    constructor : bool,
    [{}] race_results: list of 16 dicts (one dict per result type) [ {}, {} ]
        [0] [{}, {}] 
            "fantasy_results": 
                list [ 16 dicts: {'id': 'attr1', 'points_per_race_list': [X0, X1, ... Xlastrace] }, {}],
            "id": "weekend_current_points",
            "results_per_aggregation_list": [],
            "results_per_race_list": [X0, X1, ... Xlastrace]
        [1] (2023+) {} price_at_lock dict:
            dict {'id': 'price_at_lock', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
        [2] (2023+)] {} price_change:
            dict {'id': 'price_change', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
        [3] (2023+) weekend_PPM:
            dict {'id': 'weekend_PPM', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]}
        [4-15] Not Required - Actual race results, not fantasy-related
'''

#Use the generic result parser to parse the driver results
df = parse_results('driver')
#Save them using the iomanager
polars_to_parquet(filedir='data/bronze/fantasy', filename='driver_fantasy_attributes', data=df)