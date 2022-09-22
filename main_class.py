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
import numpy as np
import concurrent.futures
from itertools import repeat


# =============================================================================
# FILE IMPORTS
# =============================================================================
load_dotenv()
sys.path.append(os.path.abspath("./utils"))
from utils.jprint import jprint
from utils.time_helpers import (
    determine_cur_utc_timestamp,
    determine_today_str_timestamp,
    determine_next_midnight,
    determine_if_new_day,
    sleep_to_desired_interval,
)
from utils.discord_webhook import determine_exchange_diff_and_alert_discort

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
#   4. Error checker exception isn't thrown, since it's in try/catch
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


class ArbDataPuller:
    def __init__(self, exchanges_obj: dict, interval: int):
        self.exchanges_obj = exchanges_obj
        self.interval = interval
        self.S3_BASE_PATHS = self.determine_general_s3_filepaths()

    # =============================================================================
    # Get market data for exchanges, iterate infinitely
    # =============================================================================
    def main(self):
        self.reset_for_new_day()
        sleep_to_desired_interval(self.interval)
        while True:
            if determine_if_new_day(self.midnight):
                self.save_updated_data_to_s3()
                self.reset_for_new_day()
            self.get_bid_ask_and_process_df()
            sleep_to_desired_interval(self.interval)

    # =============================================================================
    # Get bid ask data and update dataframe obj for all exchanges
    # =============================================================================
    def get_bid_ask_and_process_df(self) -> dict:
        bid_asks = self.get_bid_ask_from_exchanges()
        determine_exchange_diff_and_alert_discort(bid_asks)
        self.update_df_obj_with_new_bid_ask_data(bid_asks)
        jprint(self.df_obj)

    # =============================================================================
    # Get current bid ask data from exchange using THREADDING
    # =============================================================================
    def get_bid_ask_from_exchanges(self) -> list:
        bid_asks = []
        now = determine_cur_utc_timestamp()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = executor.map(
                self.get_bid_ask_from_specific_exchange,
                self.exchanges_obj.items(),
                repeat(now),
            )
            for r in result:
                bid_asks.append(r)
        return bid_asks

    # =============================================================================
    # Determine the exchange and run function
    # =============================================================================
    def get_bid_ask_from_specific_exchange(
        self, exchange_and_market: tuple, now: dt
    ) -> dict:
        exchange, market = exchange_and_market[0], exchange_and_market[1]
        if exchange == "FTX_US":
            bid_ask = self.get_bid_ask_ftx(market)
        elif exchange == "DYDX":
            bid_ask = self.get_bid_ask_dydx(market)
        else:
            raise Exception("No function exists for this exchange.")

        bid_ask["exchange"] = exchange
        bid_ask["timestamp"] = now
        bid_ask["mid"] = self.compute_mid(bid_ask)
        return bid_ask

    # =============================================================================
    # Get bid/ask market data for Ftx_Us
    # =============================================================================
    def get_bid_ask_ftx(self, market: str) -> dict:
        try:
            url = f"{FTX_BASEURL}{market}/orderbook"
            res = requests.get(url).json()["result"]
            bid_ask = self.pull_best_bid_ask_from_orderbook(res, "FTX_US")
        except Exception as e:
            print(f"Exception raised in get_bid_ask_ftx(): \n{e}")
            bid_ask = self.create_nan_bid_ask_dict()
        return bid_ask

    # =============================================================================
    # Get bid/ask market data for DyDx
    # =============================================================================
    def get_bid_ask_dydx(self, market: str) -> dict:
        try:
            client = Client(host=DYDX_BASEURL)
            res = client.public.get_orderbook(market=market).data
            bid_ask = self.pull_best_bid_ask_from_orderbook(res, "DYDX")
        except Exception as e:
            print(f"Exception raised in get_bid_ask_dydx(): \n{e}")
            bid_ask = self.create_nan_bid_ask_dict()
        return bid_ask

    # =============================================================================
    # Pull best bid/ask for DyDx, verify it's sorted correctly
    # =============================================================================
    def pull_best_bid_ask_from_orderbook(self, res: list, exchange: str) -> tuple:
        asks, bids = res["asks"], res["bids"]
        if exchange == "DYDX":
            asks = [
                {"price": float(a["price"]), "size": float(a["size"])} for a in asks
            ]
            bids = [
                {"price": float(b["price"]), "size": float(b["size"])} for b in bids
            ]
        if exchange == "FTX_US":
            asks = [{"price": a[0], "size": a[1]} for a in asks]
            bids = [{"price": b[0], "size": b[1]} for b in bids]

        best_ask = asks[0]
        for ask in asks:
            if ask["price"] < best_ask["price"]:
                best_ask = ask

        best_bid = bids[0]
        for bid in bids:
            if bid["price"] > best_bid["price"]:
                best_bid = bid

        bid_ask = {
            "ask_price": best_ask["price"],
            "ask_size": best_ask["size"],
            "bid_price": best_bid["price"],
            "bid_size": best_bid["size"],
        }
        self.error_check_bid_ask_orderbook(bid_ask, exchange, asks, bids)
        return bid_ask

    # =============================================================================
    # Error check the bid ask orderbook to check for irregularities
    # =============================================================================
    def error_check_bid_ask_orderbook(
        self, bid_ask: dict, exchange: str, asks: list, bids: list
    ):
        # error checking
        if bid_ask["ask_price"] != float(asks[0]["price"]):
            print(f"{exchange} order book messed up: \n {asks}")
            raise Exception(f"{exchange} order book messed up: \n {asks}")
        if bid_ask["bid_price"] != float(bids[0]["price"]):
            print(f"{exchange} order book messed up: \n {bids}")
            raise Exception(f"{exchange} order book messed up: \n {bids}")

        diff = (bid_ask["ask_price"] / bid_ask["bid_price"] - 1) * 100
        if diff > 5:
            print(f"Warning, diff is larger than 5%: {diff}")

    # =============================================================================
    # If exchange doesn't return proper data, create nan dictionary
    # =============================================================================
    def create_nan_bid_ask_dict(self) -> dict:
        return {
            "ask_price": np.nan,
            "ask_size": np.nan,
            "bid_price": np.nan,
            "bid_size": np.nan,
        }

    # =============================================================================
    # If no data frame exists, create dataframe
    # =============================================================================
    def update_df_obj_with_new_bid_ask_data(self, bid_asks: list) -> dict:
        for bid_ask in bid_asks:
            exchange = bid_ask.pop("exchange")
            if exchange not in self.df_obj:
                df = self.create_new_df_with_bid_ask(bid_ask)
            else:
                df = self.append_existing_df_with_bid_ask(
                    bid_ask, self.df_obj[exchange]
                )
            self.df_obj[exchange] = df

    # =============================================================================
    # Start of new day, create a new df
    # =============================================================================
    def create_new_df_with_bid_ask(self, bid_ask) -> pd.DataFrame:
        return pd.DataFrame([bid_ask])

    # =============================================================================
    # Append to existing dataframe
    # =============================================================================
    def append_existing_df_with_bid_ask(self, bid_ask, df):
        return pd.concat([df, pd.DataFrame([bid_ask])], ignore_index=True)

    # =============================================================================
    # Compute mid between ask and bid_ask
    # =============================================================================
    def compute_mid(self, bid_ask: dict) -> float:
        mid = (bid_ask["ask_price"] + bid_ask["bid_price"]) / 2
        return round(mid, 3)

    # =============================================================================
    # Get all relevant filepaths to fetch and save data to
    # =============================================================================
    def determine_general_s3_filepaths(self) -> dict:
        s3_base_paths = {}
        for exchange, market in self.exchanges_obj.items():
            if "/" in market:
                market = market.replace("/", "-")
            path = f"{exchange}/{market}/{exchange}-{market}"
            s3_base_paths[exchange] = path
        return s3_base_paths

    # =============================================================================
    # Create filesnames for today's date (date in filename!)
    # =============================================================================
    def update_s3_filepaths(self):
        updated = {}
        today = determine_today_str_timestamp()
        for exchange, path in self.S3_BASE_PATHS.items():
            updated[exchange] = f"{path}-{today}.csv"
        return updated

    # =============================================================================
    # Save the updated df to S3
    # =============================================================================
    def save_updated_data_to_s3(self) -> None:
        self.prepare_df_obj_for_s3()
        for exchange, df in self.df_obj.items():
            path = self.s3_paths[exchange]
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            response = s3.put_object(
                Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
            )
            jprint(response)

    # =============================================================================
    # Preare the final df_obj to be save to S3
    # =============================================================================
    def prepare_df_obj_for_s3(self) -> dict:
        for exchange, df in self.df_obj.items():
            df = df.set_index("timestamp")
            df.index = pd.to_datetime(df.index).tz_localize(None)
            self.df_obj[exchange] = df

    # =============================================================================
    # Reset all values that start a new with new day
    # =============================================================================
    def reset_for_new_day(self) -> tuple:
        self.s3_paths = self.update_s3_filepaths()
        self.midnight = determine_next_midnight()
        self.df_obj = {}


if __name__ == "__main__":
    # to active venv: source venv/bin/activate
    exchanges_obj = {"FTX_US": "ETH/USD", "DYDX": "ETH-USD"}
    obj = ArbDataPuller(exchanges_obj=exchanges_obj, interval=5)
    obj.main()
