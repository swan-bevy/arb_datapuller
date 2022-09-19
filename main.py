# =============================================================================
# IMPORTS
# =============================================================================
import os, sys
import boto3
from io import StringIO
import datetime as dt
from dotenv import load_dotenv
import requests
from dydx3 import Client
import pandas as pd
import concurrent.futures

# =============================================================================
# FILE IMPORTS
# =============================================================================
load_dotenv()
sys.path.append(os.path.abspath("./utils"))
sys.path.append(os.path.abspath("./clients"))
from clients.FtxClient import FtxClient
from utils.pprint_v2 import pprint

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
# CONSTANTS
# =============================================================================
BUCKET_NAME = "arb-live-data"
TIMESTAMP = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

# =============================================================================
# Get market data for Ftx.US
# =============================================================================
def main(exchanges_obj: dict):  # exchanges = { 'DYDX': 'BTC-USD' }
    bid_ask = get_current_data_from_exchanges(exchanges_obj)
    s3_paths = get_s3_filepaths(exchanges_obj)
    df_obj = get_csv_files_from_s3(s3_paths)
    df_obj = append_bid_ask_data_to_df(bid_ask, df_obj)
    save_updated_data_to_s3(s3_paths, df_obj)


# =============================================================================
# Get data and store in dict
# =============================================================================
def get_current_data_from_exchanges(exchanges_obj: dict) -> list:
    bid_ask = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = executor.map(get_data_from_specific_exchange, exchanges_obj.items())
        for r in result:
            bid_ask.append(r)
    return bid_ask


# =============================================================================
# Determine the exchange and run function
# =============================================================================
def get_data_from_specific_exchange(exchange_market: tuple) -> dict:
    exchange, market = exchange_market[0], exchange_market[1]
    if exchange == "FTX.US":
        return get_bid_ask_ftx(market)
    elif exchange == "DYDX":
        return get_bid_ask_dydx(market)
    else:
        raise Exception("No function exists for this exchange.")


# =============================================================================
# Get market data for Ftx.US
# =============================================================================
def get_bid_ask_ftx(market: str) -> dict:
    response = requests.get(f"https://ftx.us/api/markets/{market}").json()
    response = response["result"]
    ask, bid = response["ask"], response["bid"]
    return {"exchange": "FTX.US", "ask": ask, "bid": bid, "timestamp": TIMESTAMP}


# =============================================================================
# Get market data for DyDx
# =============================================================================
def get_bid_ask_dydx(market: str) -> dict:
    client = Client(host="https://api.dydx.exchange")
    res = client.public.get_orderbook(market=market).data

    ask = min([float(v["price"]) for v in res["asks"]])
    bid = max([float(v["price"]) for v in res["bids"]])

    # Checking validity, since we need to determine spread ourselves
    if ask != float(res["asks"][0]["price"]):
        raise Exception("Error determining ask price.")
    if bid != float(res["bids"][0]["price"]):
        raise Exception("Error determining bis price.")

    return {"exchange": "DYDX", "ask": ask, "bid": bid, "timestamp": TIMESTAMP}


# =============================================================================
# Get all relevant filepaths to fetch and save data to
# =============================================================================
def get_s3_filepaths(exchanges_obj: dict) -> dict:
    s3_paths = {}
    for exchange, market in exchanges_obj.items():
        if "/" in market:
            market = market.replace("/", "-")
        path = f"{exchange}/{market}.csv"
        s3_paths[exchange] = path
    return s3_paths


# =============================================================================
# Get the CSV files with current data from S3
# =============================================================================
def get_csv_files_from_s3(s3_paths: dict) -> dict:
    df_obj = {}
    for exchange, path in s3_paths.items():
        res = s3.get_object(Bucket=BUCKET_NAME, Key=path)
        df = pd.read_csv(res["Body"])
        df_obj[exchange] = df
    return df_obj


# =============================================================================
# Append the newest data to the dfs, so we can save to S3
# =============================================================================
def append_bid_ask_data_to_df(bid_ask: list, df_obj: dict) -> dict:
    for bd in bid_ask:
        exchange = bd.pop("exchange")
        df = df_obj[exchange]
        new_row = pd.DataFrame([bd])
        df = pd.concat([df, pd.DataFrame([bd])], ignore_index=True)
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index)
        df_obj[exchange] = df
    return df_obj


# =============================================================================
# Save the updated df to S3
# =============================================================================
def save_updated_data_to_s3(s3_paths: dict, df_obj: dict) -> None:
    for exchange, path in s3_paths.items():
        df = df_obj[exchange]
        print(df)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        response = s3.put_object(
            Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
        )


if __name__ == "__main__":
    exchanges_obj = {"FTX.US": "ETH/USD", "DYDX": "ETH-USD"}
    main(exchanges_obj=exchanges_obj)
