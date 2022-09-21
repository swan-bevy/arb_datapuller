# =============================================================================
# IMPORTS
# =============================================================================
import os, sys, time
import boto3
from io import StringIO
import datetime as dt
from dotenv import load_dotenv
import requests
from dydx3 import Client
import pandas as pd
import numpy as np
import concurrent.futures
from itertools import repeat

# =============================================================================
# FILE IMPORTS
# =============================================================================
load_dotenv()
sys.path.append(os.path.abspath("./utils"))
from utils.jprint import jprint

# =============================================================================
# AWS CONFIG
# =============================================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="eu-central-1",
)

# =============================================================================
# ISSUES
#   1. Should I use the same timestamp variable for both exchanges, or should
#      I determine a new timestamp variable for every exchange? Probably the same
#      to allow easy merging of dfs later
#   2. I use floats not decimals for mid calculation. Should be fine tho
#   3. Verify if df appending/concatening is correct
#   4. I don't check for min ask, max bid price, I just take the first values in the list
# =============================================================================

# =============================================================================
# CONSTANTS
# =============================================================================
BUCKET_NAME = "arb-live-data"
FTX_BASEURL = "https://ftx.us/api/markets/"
DYDX_BASEURL = "https://api.dydx.exchange"  # No "/" at end!


# =============================================================================
# Get market data for exchanges, iterate infinitely
# Exchanges_obj is a dict with exchange as key, and pair/market as value
# Interval is an integer representing seconds
# =============================================================================
def main(exchanges_obj: dict, interval: int):
    S3_BASE_PATHS = determine_general_s3_filepaths(exchanges_obj)  # CONSTANT
    s3_paths, midnight, df_obj = reset_for_new_day(S3_BASE_PATHS)
    sleep_to_desired_interval(interval)
    while True:
        df_obj = get_bid_ask_and_process_df(exchanges_obj, df_obj)
        if determine_if_new_day(midnight):
            save_updated_data_to_s3(s3_paths, df_obj)
            s3_paths, midnight, df_obj = reset_for_new_day(S3_BASE_PATHS)
        sleep_to_desired_interval(interval)


# =============================================================================
# Get bid ask data and update dataframe obj for all exchanges
# =============================================================================
def get_bid_ask_and_process_df(exchanges_obj: dict, df_obj: dict) -> dict:
    bid_asks = get_bid_ask_from_exchanges(exchanges_obj)
    df_obj = update_df_obj_with_new_bid_ask_data(df_obj, bid_asks)
    jprint(df_obj)
    return df_obj


# =============================================================================
# Get current bid ask data from exchange using THREADDING
# =============================================================================
def get_bid_ask_from_exchanges(exchanges_obj: dict) -> list:
    bid_asks = []
    now = determine_cur_utc_timestamp()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = executor.map(
            get_bid_ask_from_specific_exchange, exchanges_obj.items(), repeat(now)
        )
        for r in result:
            bid_asks.append(r)
    return bid_asks


# =============================================================================
# Determine the exchange and run function
# =============================================================================
def get_bid_ask_from_specific_exchange(exchange_and_market: tuple, now: dt) -> dict:
    exchange, market = exchange_and_market[0], exchange_and_market[1]
    if exchange == "FTX_US":
        bid_ask = get_bid_ask_ftx(market)
    elif exchange == "DYDX":
        bid_ask = get_bid_ask_dydx(market)
    else:
        raise Exception("No function exists for this exchange.")

    bid_ask["exchange"] = exchange
    bid_ask["timestamp"] = now
    bid_ask["mid"] = compute_mid(bid_ask)
    return bid_ask


# =============================================================================
# Get bid/ask market data for Ftx_Us
# =============================================================================
def get_bid_ask_ftx(market: str) -> dict:
    bid_ask = {}
    try:
        url = f"{FTX_BASEURL}{market}/orderbook"
        res = requests.get(url).json()
        ask, bid = res["result"]["asks"][0], res["result"]["bids"][0]
        bid_ask["ask_price"] = ask[0]
        bid_ask["ask_size"] = ask[1]
        bid_ask["bid_price"] = bid[0]
        bid_ask["bid_size"] = bid[1]
    except Exception as e:
        print(e)
        bid_ask = create_nan_bid_ask_dict()
    return bid_ask


# =============================================================================
# Get bid/ask market data for DyDx
# =============================================================================
def get_bid_ask_dydx(market: str) -> dict:
    bid_ask = {}
    try:
        client = Client(host=DYDX_BASEURL)
        res = client.public.get_orderbook(market=market).data
        ask, bid = res["asks"][0], res["bids"][0]
        bid_ask["ask_price"] = float(ask["price"])
        bid_ask["ask_size"] = float(ask["size"])
        bid_ask["bid_price"] = float(bid["price"])
        bid_ask["bid_size"] = float(bid["size"])
    except Exception as e:
        print(e)
        bid_ask = create_nan_bid_ask_dict()
    return bid_ask


# =============================================================================
# If exchange doesn't return proper data, create nan dictionary
# =============================================================================
def create_nan_bid_ask_dict() -> dict:
    return {
        "ask_price": np.nan,
        "ask_size": np.nan,
        "bid_price": np.nan,
        "bid_size": np.nan,
    }


# =============================================================================
# If no data frame exists, create dataframe
# =============================================================================
def update_df_obj_with_new_bid_ask_data(df_obj: dict, bid_asks: list) -> dict:
    for bid_ask in bid_asks:
        exchange = bid_ask.pop("exchange")
        if exchange not in df_obj:
            df = create_new_df_with_bid_ask(bid_ask)
        else:
            df = append_existing_df_with_bid_ask(bid_ask, df_obj[exchange])
        df_obj[exchange] = df
    return df_obj


# =============================================================================
# Start of new day, create a new df
# =============================================================================
def create_new_df_with_bid_ask(bid_ask) -> pd.DataFrame:
    return pd.DataFrame([bid_ask])


# =============================================================================
# Append to existing dataframe
# =============================================================================
def append_existing_df_with_bid_ask(bid_ask, df):
    return pd.concat([df, pd.DataFrame([bid_ask])], ignore_index=True)


# =============================================================================
# Compute mid between ask and bid_ask
# =============================================================================
def compute_mid(bid_ask: dict) -> float:
    mid = (bid_ask["ask_price"] + bid_ask["bid_price"]) / 2
    return round(mid, 3)


# =============================================================================
# Get all relevant filepaths to fetch and save data to
# =============================================================================
def determine_general_s3_filepaths(exchanges_obj: dict) -> dict:
    s3_base_paths = {}
    for exchange, market in exchanges_obj.items():
        if "/" in market:
            market = market.replace("/", "-")
        path = f"{exchange}/{market}/{exchange}-{market}"
        s3_base_paths[exchange] = path
    return s3_base_paths


# =============================================================================
# Create filesnames for today's date (date in filename!)
# =============================================================================
def update_s3_filepaths(s3_base_paths: dict):
    updated = {}
    today = determine_today_str_timestamp()
    for exchange, path in s3_base_paths.items():
        updated[exchange] = f"{path}-{today}.csv"
    return updated


# =============================================================================
# Save the updated df to S3
# =============================================================================
def save_updated_data_to_s3(s3_paths: dict, df_obj: dict) -> None:
    df_obj = prepare_df_obj_for_s3(df_obj)
    for exchange, df in df_obj.items():
        path = s3_paths[exchange]
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        response = s3.put_object(
            Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
        )
        jprint(response)


# =============================================================================
# Preare the final df_obj to be save to S3
# =============================================================================
def prepare_df_obj_for_s3(df_obj: dict) -> dict:
    for exchange, df in df_obj.items():
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df_obj[exchange] = df
    return df_obj


# =============================================================================
# Reset all values that start a new with new day
# =============================================================================
def reset_for_new_day(s3_base_paths: dict) -> tuple:
    s3_paths = update_s3_filepaths(s3_base_paths)
    midnight = determine_next_midnight()
    df_obj = {}
    return s3_paths, midnight, df_obj


# =============================================================================
#
# HELPERS
#
# =============================================================================


# =============================================================================
# Generate cur datetime object
# =============================================================================
def determine_cur_utc_timestamp() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# =============================================================================
# Generate dt str for today at midnight (e.g. `2022-09-20`)
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


if __name__ == "__main__":
    exchanges_obj = {"FTX_US": "ETH/USD", "DYDX": "ETH-USD"}
    main(exchanges_obj=exchanges_obj, interval=5)
