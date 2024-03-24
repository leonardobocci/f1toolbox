import fastf1
import flatdict

import polars as pl

from fastf1.core import DataNotLoadedError
from utils.iomanager import save_raw_fastf1_json as save_json
from utils.iomanager import polars_to_parquet
from datetime import datetime
from assets import constants

def _extract_event(session: fastf1.core.Session,year: int) -> dict:
    '''Extract event info from a session object and save to landing zone'''
    event = {
        'id': session.session_info['Meeting']['Key'],
        'name': session.session_info['Meeting']['Name'],
        'Country': session.session_info['Meeting']['Country'],
        'Circuit': session.session_info['Meeting']['Circuit'],
    }
    flat_event = flatdict.FlatDict(event)
    flat_event.set_delimiter('_')
    flat_event = dict(flat_event)
    save_json(flat_event, str(event["id"]), year, 'events')
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
    return flat_event

def _extract_session(session: fastf1.core.Session, event_info: dict, year: int) -> int:
    '''Extract session data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved.'''
    session = {
        'id': session.session_info['Key'],
        'event_id': event_info['id'],
        'name': session.session_info['Name'],
        'type': session.session_info['Type'],
        'number': session.session_info['Number'],
        'start_date': session.session_info['StartDate'].isoformat(),
        'end_date': session.session_info['EndDate'].isoformat(),
        'local_timezone_utc_offset': str(session.session_info['GmtOffset']),
    }
    save_json(session, session["id"], year, 'sessions')
    return session['id']

def _extract_session_weather(session: fastf1.core.Session, event_info: dict, year: int) -> int:
    '''Extract weather data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved.'''
    weather = pl.LazyFrame(session.weather_data)
    session_id = session.session_info['Key']
    weather.with_columns(pl.lit(session_id).alias('session_id'))
    weather.with_columns(pl.lit(session.session_info['Meeting']['Key']).alias('event_id'))
    polars_to_parquet(filedir=f'{constants.RAW_FASTF1_PATH}/{year}/weathers/{session_id}', filename='weather', data=weather)   
    return session_id

def extract_fastf1(context, year: int, event_num: int = 1) -> dict:
    ''' 
    Extract all race events in a year and save to landing zone.
    '''
    extraction_metadata = {
        "saved_events": [],
        "total_events": 0,
        "saved_sessions": [],
        "saved_weather_sessions": []
    }
    event_calendar = fastf1.get_event_schedule(year)
    num_events = len(event_calendar.loc[event_calendar['EventFormat'] != 'testing'])
    extraction_metadata['total_events']= num_events
    if year == datetime.today().year:
        remaining_num_events = len(fastf1.get_events_remaining(dt=datetime.today(), include_testing=False))
        num_events = num_events - remaining_num_events
    context.log.info(f'Extracting {num_events} events for {year}')
    while event_num <= num_events:
        session = fastf1.get_session(year=year, gp=event_num, identifier=1)
        if not session.f1_api_support:
            raise Exception('Fast F1 does not support this session')
        session.load(laps=False, telemetry=True, weather=True, messages=False)
        #Collect general event information
        event_info = _extract_event(session, year)
        extraction_metadata['saved_events'].append(f'{year}_{event_num}')
        context.log.info(f'{year}_{event_num} saved')
        #Collect session information
        saved_session = _extract_session(session, event_info, year)
        extraction_metadata['saved_sessions'].append(saved_session)
        context.log.info(f'{year}_{event_num} session saved')
        #Collect event weather data
        saved_weather_session = _extract_session_weather(session, event_info, year)
        extraction_metadata['saved_weather_sessions'].append(saved_weather_session)
        context.log.info(f'{year}_{event_num} weather saved')
        event_num += 1
    return extraction_metadata