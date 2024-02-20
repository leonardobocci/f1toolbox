years = [2022, 2023]

season_metadata = {
    #valid since 2018
    'latest': {
        'drivers': 20,
        'constructors': 10
    }
}

driver_schema_contract = {
    'base_keys': ['abbreviation', 'color', 'constructor', 'race_results'],
    'fantasy_results_expectations': {
        'id': 'weekend_current_points',
        'len': 16,
        'entries': [
            'quali_not_classified_points',
            'quali_disqualified_points',
            'quali_pos_points',
            'sprint_pos_gained_points',
            'sprint_overtake_points',
            'sprint_fastest_lap_points',
            'sprint_not_classified_points',
            'sprint_disqualified_points',
            'sprint_pos_points',
            'race_pos_gained_points',
            'race_overtake_points',
            'race_fastest_lap_points',
            'race_not_classified_points',
            'race_disqualified_points',
            'race_pos_points',
            'race_dotd_points'
        ]
    }
}

constructor_schema_contract = {
    'base_keys': ['abbreviation', 'color', 'constructor', 'race_results'],
    'fantasy_results_expectations': {
        'id': 'weekend_current_points',
        'len': 17,
        'entries': [
            'quali_not_classified_points',
            'quali_disqualified_points',
            'quali_pos_points',
            'quali_teamwork_points',
            'sprint_pos_gained_points',
            'sprint_overtake_points',
            'sprint_fastest_lap_points',
            'sprint_not_classified_points',
            'sprint_disqualified_points',
            'sprint_pos_points',
            'race_pos_gained_points',
            'race_overtake_points',
            'race_fastest_lap_points',
            'race_not_classified_points',
            'race_disqualified_points',
            'race_pos_points',
            'race_pit_stop_points'
        ]
    }
}