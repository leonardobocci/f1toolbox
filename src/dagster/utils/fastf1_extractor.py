import functools
import time
from datetime import datetime

import fastf1
import fastf1.plotting as f1plot
import flatdict
import polars as pl

from src.dagster.assets import constants


def retry(exception_to_check, tries=3, delay=1):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _tries = tries
            # Try to extract context from either positional or keyword arguments
            context = None
            if "context" in kwargs:
                context = kwargs["context"]
            elif len(args) > 0:
                context = args[0]  # Assuming context is the first positional argument

            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    context.log.warning(f"Retrying exception: {str(e)}")
                    context.log.info(
                        f"Retrying in {delay} seconds... ({_tries-1} tries left)"
                    )
                    time.sleep(delay)
                    _tries -= 1
                    if exception_to_check == fastf1.core.DataNotLoadedError:
                        context.log.info("Clearing fastf1 cache")
            # Last attempt (no more retries)
            return func(*args, **kwargs)

        return wrapper

    return decorator_retry


@retry(exception_to_check=fastf1.core.DataNotLoadedError, tries=5, delay=10)
def extract_event(
    context, year: int, event_num: int, meta
) -> tuple[dict, pl.DataFrame]:
    """Extract event info from a session object and save to landing zone"""

    def extract_circuit_info(year: int, circuit_key: int) -> tuple[pl.DataFrame, int]:
        circuit_info = fastf1.mvapi.get_circuit_info(year=year, circuit_key=circuit_key)
        corners = pl.DataFrame(circuit_info.corners)
        track_map_rotation_degrees = circuit_info.rotation
        return corners, track_map_rotation_degrees

    session = fastf1.get_session(year=year, gp=event_num, identifier=4)
    if not session.f1_api_support:
        raise Exception("Fast F1 does not support this session")
    session.load(laps=False, telemetry=False, weather=False, messages=False)
    event = {
        "id": session.session_info["Meeting"]["Key"],
        "name": session.session_info["Meeting"]["Name"],
        "Country": session.session_info["Meeting"]["Country"],
        "Circuit": session.session_info["Meeting"]["Circuit"],
    }
    flat_event = flatdict.FlatDict(event)
    flat_event.set_delimiter("_")
    flat_event = dict(flat_event)
    flat_event["season"] = year
    flat_event["round_number"] = event_num

    # Extract circuit information
    circuit_key = flat_event["Circuit_Key"]
    corners, track_map_rotation_degrees = extract_circuit_info(year, circuit_key)
    flat_event["track_map_rotation_degrees"] = track_map_rotation_degrees
    meta.extraction_metadata["saved_events"].append(flat_event["id"])

    corners = corners.with_columns(pl.lit(year).alias("year"))
    corners = corners.with_columns(pl.lit(circuit_key).alias("circuit_key"))
    corners = corners.with_columns(pl.col("X").shift(1, fill_value=0).alias("x_prev"))
    corners = corners.with_columns(pl.col("Y").shift(1, fill_value=0).alias("y_prev"))
    # https://en.wikipedia.org/wiki/Euclidean_distance
    corners = corners.with_columns(
        ((pl.col("X") - pl.col("x_prev")) ** 2 + (pl.col("Y") - pl.col("y_prev")) ** 2)
        .sqrt()
        .alias("distance_from_last_corner")
    )
    corners = corners.drop(["x_prev", "y_prev"])
    meta.extraction_metadata["saved_circuit_corners"].append(f"{year}_{circuit_key}")
    """
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
    """
    return flat_event, corners


def _extract_session(
    session: fastf1.core.Session, session_num: int, event_id: str
) -> dict:
    """Extract session data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved."""
    session = {
        "id": session.session_info["Key"],
        "session_number": session_num,
        "event_id": event_id,
        "name": session.session_info["Name"],
        "type": session.session_info["Type"],
        "start_date": session.session_info["StartDate"].isoformat(),
        "end_date": session.session_info["EndDate"].isoformat(),
        "local_timezone_utc_offset": str(session.session_info["GmtOffset"]),
    }
    return session


def _extract_session_weather(
    session: fastf1.core.Session,
) -> tuple[int, pl.LazyFrame] | None:
    """Extract weather data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved."""
    session_id = session.session_info["Key"]
    try:
        weather = pl.LazyFrame(session.weather_data)
    except fastf1.core.DataNotLoadedError:
        return None, None
    weather = weather.with_columns(pl.lit(session_id).alias("session_id"))
    weather = weather.with_columns(
        pl.lit(session.session_info["Meeting"]["Key"]).alias("event_id")
    )
    return session_id, weather


def _extract_session_results(session: fastf1.core.Session) -> tuple[int, pl.LazyFrame]:
    """Extract results data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved."""
    results = pl.LazyFrame(session.results)
    session_id = session.session_info["Key"]
    results = results.with_columns(pl.lit(session_id).alias("session_id"))
    results = results.with_columns(
        pl.lit(session.session_info["Meeting"]["Key"]).alias("event_id")
    )
    return session_id, results


def _extract_session_laps(session: fastf1.core.Session) -> tuple[int, pl.LazyFrame]:
    """Extract lap data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved."""
    laps = pl.LazyFrame(session.laps).with_columns(pl.col("Deleted").cast(pl.Utf8))
    session_id = session.session_info["Key"]
    laps = laps.with_columns(pl.lit(session_id).alias("session_id"))
    laps = laps.with_columns(
        pl.lit(session.session_info["Meeting"]["Key"]).alias("event_id")
    )
    return session_id, laps


def _extract_session_telemetry(
    session: fastf1.core.Session,
) -> tuple[int, pl.LazyFrame] | None:
    """Extract telemetry data from a session object and save to landing zone (parquet).
    The file path includes the session id.
    Returns the session id that was saved."""
    car_data = session.car_data
    pos_data = session.pos_data
    telemetry = pl.LazyFrame()
    if not (car_data and pos_data):
        # Telemetry is not available
        return None, None
    for key in session.car_data.keys():
        # recast to Nullable float type to remove dtype warnings
        session.pos_data[key]["X"] = session.pos_data[key]["X"].astype("Float64")
        session.pos_data[key]["Y"] = session.pos_data[key]["Y"].astype("Float64")
        session.pos_data[key]["Z"] = session.pos_data[key]["Z"].astype("Float64")
        car_telemetry = pl.LazyFrame(
            session.pos_data[key].merge_channels(session.car_data[key])
        )
        car_telemetry = car_telemetry.with_columns(pl.lit(key).alias("car_number"))
        if telemetry.width:
            telemetry = pl.concat([telemetry, car_telemetry], how="vertical_relaxed")
        else:
            telemetry = car_telemetry
    session_id = session.session_info["Key"]
    telemetry = telemetry.with_columns(pl.lit(session_id).alias("session_id"))
    return session_id, telemetry


def extract_tyre_compounds(year: int, meta) -> pl.DataFrame:
    session = fastf1.get_session(year=year, gp=1, identifier=5)
    tyre_dict = f1plot.get_compound_mapping(session)
    tyre_compounds = (
        pl.from_dict(tyre_dict)
        .transpose(include_header=True, header_name="compound")
        .rename({"column_0": "color"})
    )
    tyre_compounds = tyre_compounds.with_columns(pl.lit(year).alias("season"))
    meta.extraction_metadata["saved_tyres"].append({year: list(tyre_dict.keys())})
    return tyre_compounds


@retry(exception_to_check=fastf1.core.DataNotLoadedError, tries=5, delay=10)
def extract_fastf1_signals(
    context,
    year: int,
    session_num: int,
    event_num: int,
    event_id: str,
    meta: dict,
) -> tuple[dict, pl.LazyFrame, pl.LazyFrame | None, pl.LazyFrame, pl.LazyFrame | None]:
    session = fastf1.get_session(year=year, gp=event_num, identifier=session_num)
    session.load(laps=True, telemetry=True, weather=True, messages=False)
    # Collect session information
    saved_session = _extract_session(session, session_num, event_id)
    meta.extraction_metadata["saved_sessions"].append(saved_session["id"])
    context.log.info(f"{year}_{event_num}_{session_num} session found")
    # Collect session results
    saved_results_session, saved_session_results = _extract_session_results(session)
    meta.extraction_metadata["saved_results"].append(saved_results_session)
    context.log.info(f"{year}_{event_num}_{session_num} results found")
    # Collect event weather data
    saved_weather_session, saved_weather = _extract_session_weather(session)
    if not saved_weather_session:
        meta.extraction_metadata["session_level_errors"].append(
            f"missing_weather_{year}_{event_num}_{session_num}"
        )
        context.log.error(f"{year}_{event_num}_{session_num} weather not available")
    else:
        meta.extraction_metadata["saved_weathers"].append(saved_weather_session)
        context.log.info(f"{year}_{event_num}_{session_num} weather found")
    # Collect lap data
    saved_lap_session, saved_laps = _extract_session_laps(session)
    meta.extraction_metadata["saved_laps"].append(saved_lap_session)
    context.log.info(f"{year}_{event_num}_{session_num} laps found")
    # Collect telemetry data
    saved_telemetry_session, saved_telemetry = _extract_session_telemetry(session)
    if not saved_telemetry_session:
        meta.extraction_metadata["session_level_errors"].append(
            f"missing_telemetry_{year}_{event_num}_{session_num}"
        )
        context.log.error(f"{year}_{event_num}_{session_num} telemetry not available")
    else:
        meta.extraction_metadata["saved_telemetry"].append(saved_telemetry_session)
        context.log.info(f"{year}_{event_num}_{session_num} telemetry found")
    return (
        saved_session,
        saved_session_results,
        saved_weather,
        saved_laps,
        saved_telemetry,
    )


def get_num_events(year: int, meta) -> int:
    event_calendar = fastf1.get_event_schedule(year)
    num_events = len(event_calendar.loc[event_calendar["EventFormat"] != "testing"])
    meta.extraction_metadata["total_events"] = num_events
    if year == datetime.today().year:
        remaining_num_events = len(
            fastf1.get_events_remaining(dt=datetime.today(), include_testing=False)
        )
        num_events = num_events - remaining_num_events
    return num_events


class ExtractionMetadata:
    def __init__(self) -> None:
        self.extraction_metadata = {
            "saved_events": [],
            "total_events": 0,
            "saved_sessions": [],
            "saved_weathers": [],
            "saved_laps": [],
            "saved_telemetry": [],
            "saved_results": [],
            "saved_circuit_corners": [],
            "saved_tyres": [],
            "session_level_errors": [],
        }


def extract_fastf1(
    context, year: int, event_num: int = 1
) -> tuple[
    dict,
    pl.DataFrame,
    dict,
    pl.DataFrame,
    dict,
    pl.LazyFrame,
    pl.LazyFrame | None,
    pl.LazyFrame,
    pl.LazyFrame | None,
]:
    """
    Extract all race events in a year and save to landing zone.
    """
    meta = ExtractionMetadata()
    num_events = get_num_events(year, meta)
    # get tyre compounds for this season
    tyre_compounds = extract_tyre_compounds(year, meta)
    context.log.info(f"Extracting {num_events} events for {year}")

    event_info = []
    circuit_corners = pl.LazyFrame()
    sessions = []
    session_results = pl.LazyFrame()
    weather = pl.LazyFrame()
    laps = pl.LazyFrame()
    telemetry = pl.LazyFrame()

    def create_or_concat_df(df, extension_df):
        if df.collect_schema():
            return pl.concat([df, extension_df])
        else:
            return extension_df

    while event_num <= num_events:
        # Collect general event information
        _event_info, _circuit_corners = extract_event(context, year, event_num, meta)
        _event_id = _event_info["id"]
        context.log.info(f"{year}_{event_num} event details found")
        for session_num in constants.SESSIONS:
            _sessions, _session_results, _weather, _laps, _telemetry = (
                extract_fastf1_signals(
                    context, year, session_num, event_num, _event_id, meta
                )
            )
        context.log.info(f"{year}_{event_num} event signals found")
        event_info.append(_event_info)
        circuit_corners = create_or_concat_df(circuit_corners, _circuit_corners)
        sessions.append(_sessions)
        session_results = create_or_concat_df(session_results, _session_results)
        weather = create_or_concat_df(weather, _weather)
        laps = create_or_concat_df(laps, _laps)
        telemetry = create_or_concat_df(telemetry, _telemetry)
        event_num += 1
    return (
        meta.extraction_metadata,
        tyre_compounds,
        event_info,
        circuit_corners,
        sessions,
        session_results,
        weather,
        laps,
        telemetry,
    )
