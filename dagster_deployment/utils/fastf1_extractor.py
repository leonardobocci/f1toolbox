import fastf1
import flatdict
import logging

from fastf1.core import DataNotLoadedError
from utils.iomanager import save_raw_fastf1_json as save_json
from datetime import datetime

def _extract_event(session: fastf1.core.Session) -> None:
    '''Extract event info from a session object and save to landing zone'''
    event = {
        'name': session.session_info['Meeting']['Name'],
        'Country': session.session_info['Meeting']['Country'],
        'Circuit': session.session_info['Meeting']['Circuit'],
    }
    flat_event = flatdict.FlatDict(event)
    flat_event.set_delimiter('_')
    save_json(dict(flat_event), f'{event["name"]}_event_details')
    '''
    Session Object
    ~ if useful
    
    attributes:

    event: The grand prix weekend event or testing event
    ~ name: Segment name (eg. Qualifying or FP1)
    ~ f1_api_support: bool indicating telemetry support
    ~ date: timestamp with segment start time
    api_path: static server path
    ~ session_info: dict with event and session details
    drivers: list of driver number stings
    ~ results: df with driver information and session results (always has all columns, many will be empty) - refer to https://docs.fastf1.dev/core.html#fastf1.core.SessionResults
    ~ laps: object with aggregate lap data
    total_laps: expected number of laps in the session
    ~ weather_data: dataframe with weather
    ~ car_data: dict with dataframes containing telemetry data for each car
    ~ pos_data: dict with dataframes containing XYZ position data for each car
    session_status: df with polled session status
    track_status: df with polled track status (yellow, red, etc.)
    race_control_messages: df with broadcasted messages (flags, start/end, drs enabled, etc,)

    Methods:
    ~ load(): load session data
    get_driver(identifier): get driver object by number or name
    ~ get_circuit_info(): get circuit information
    '''
    return

def extract_race_events(year: int, event_num: int = 1) -> dict:
    ''' 
    Extract all race events in a year and save to landing zone.
    '''
    events = {
        "saved": [],
        "total": 0
    }
    event_calendar = fastf1.get_event_schedule(year)
    num_events = len(event_calendar.loc[event_calendar['EventFormat'] != 'testing'])
    events['total']= num_events
    if year == datetime.today().year:
        remaining_num_events = len(fastf1.get_events_remaining(dt=datetime.today(), include_testing=False))
        num_events = num_events - remaining_num_events
    logging.info(f'Extracting {num_events} events for {year}')
    while event_num <= num_events:
        session = fastf1.get_session(year=year, gp=event_num, identifier=1)
        if not session.f1_api_support:
            raise Exception('Fast F1 does not support this session')
        session.load(laps=False, telemetry=False, weather=True, messages=False)
        _extract_event(session)
        events['saved'].append(f'{year}_{event_num}')
        logging.info(f'{year}_{event_num} saved')
        event_num += 1
    return events

