import datetime as dt
import time

# =============================================================================
# Generate cur datetime object
# =============================================================================
def determine_cur_utc_timestamp() -> dt.datetime:
    return dt.datetime.utcnow()


# =============================================================================
# Generate cur datetime object, convert to string
# =============================================================================
def determine_cur_utc_timestamp_as_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# Generate dt str for today at midnight (format: `2022-09-20`), w/o h, m, s
# =============================================================================
def determine_today_str_timestamp() -> str:
    cur = determine_cur_utc_timestamp()
    today = cur.replace(hour=0, minute=0, second=0, microsecond=0)
    return today.strftime("%Y-%m-%d")


# =============================================================================
# Determine next midnight, gives datetime_object
# =============================================================================
def determine_next_midnight() -> dt.datetime:
    now = determine_cur_utc_timestamp()
    date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight = date + dt.timedelta(days=1)
    return midnight


# =============================================================================
# Determines if we passed current midnight
# =============================================================================
def determine_if_new_day(midnight: dt.datetime) -> bool:
    return determine_cur_utc_timestamp() >= midnight


# =============================================================================
# Sleep until top of minute, hour, etc
# =============================================================================
def sleep_to_desired_interval(interval: int):
    time.sleep(float(interval) - (time.time() % float(interval)))


# =============================================================================
# Convert timestamp to date (e.g. 2022-09-23), ignore hr, min, sec
# =============================================================================
def convert_timestamp_to_today_date(timestamp):
    return timestamp.strftime("%Y-%m-%d")


# =============================================================================
# Convert timestamp to date (e.g. 2022-09-23), ignore hr, min, sec
# =============================================================================
def convert_datetime_str_to_obj(s):
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
