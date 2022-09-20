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
from utils.pprint_v2 import pprint_v2 as pprint

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
#   3. The way it's constructed currently, we'll always lose a minute at midnight
#   4. Verify if df appending/concatening is correct
# =============================================================================

# =============================================================================
# CONSTANTS
# =============================================================================
BUCKET_NAME = "arb-live-data"
FTX_BASEURL = "https://ftx.us/api/markets/"
DYDX_BASEURL = "https://api.dydx.exchange"


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
    pprint(df_obj)
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
def get_bid_ask_from_specific_exchange(exchange_and_market: tuple, now: object) -> dict:
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
    try:
        response = requests.get(f"{FTX_BASEURL}{market}").json()
        response = response["result"]
        ask = response["ask"]
        bid = response["bid"]
    except Exception as e:
        print(e)
        return {"ask": np.nan, "bid": np.nan}
    return {"ask": ask, "bid": bid}


# =============================================================================
# Get bid/ask market data for DyDx
# =============================================================================
def get_bid_ask_dydx(market: str) -> dict:
    try:
        client = Client(host=DYDX_BASEURL)
        res = client.public.get_orderbook(market=market).data
        ask = min([float(v["price"]) for v in res["asks"]])
        bid = max([float(v["price"]) for v in res["bids"]])
    except Exception as e:
        print(e)
        return {"ask": np.nan, "bid": np.nan}

    # Checking validity, since we need to determine spread ourselves
    if ask != float(res["asks"][0]["price"]):
        raise Exception("Error determining ask price.")
    if bid != float(res["bids"][0]["price"]):
        raise Exception("Error determining bis price.")
    return {"ask": ask, "bid": bid}


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
    mid = (bid_ask["ask"] + bid_ask["bid"]) / 2
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
    today = determine_midnight_today_str_timestamp()
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
        pprint(response)


# =============================================================================
# Preare the final df_obj to be save to S3
# =============================================================================
def prepare_df_obj_for_s3(df_obj: dict) -> dict:
    for exchange, df in df_obj.items():
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index)
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
# Generate dt str for today at midnight
# =============================================================================
def determine_midnight_today_str_timestamp() -> str:
    cur = determine_cur_utc_timestamp()
    today = cur.replace(hour=0, minute=0, second=0, microsecond=0)
    return today.strftime("%Y-%m-%d")


# =============================================================================
# Determine next midnight
# =============================================================================
def determine_next_midnight() -> dt.datetime:
    now = determine_cur_utc_timestamp()
    date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight = date + dt.timedelta(days=1)
    return midnight


# =============================================================================
# Determines if we passed current midnight
# =============================================================================
def determine_if_new_day(midnight: object) -> bool:
    return determine_cur_utc_timestamp() >= midnight


# =============================================================================
# Sleep until top of minute, hour, etc
# =============================================================================
def sleep_to_desired_interval(interval: int):
    time.sleep(float(interval) - (time.time() % float(interval)))


if __name__ == "__main__":
    exchanges_obj = {"FTX_US": "ETH/USD", "DYDX": "ETH-USD"}
    main(exchanges_obj=exchanges_obj, interval=5)
