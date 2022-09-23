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

from sympy import EX


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

from DiscordAlert import DiscordAlert
from ArbDiff import ArbDiff

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
# Pull bid/ask from exchanges, save to S3 at midnight
# =============================================================================
class ArbDataPuller:
    def __init__(self, exchanges_obj: dict, interval: int):
        self.exchanges_obj = exchanges_obj
        self.exchanges = self.make_list_of_exchanges(exchanges_obj)
        self.diff_pairs = self.create_unique_exchange_pairs()
        self.market = self.determine_market()
        self.interval = interval
        self.S3_BASE_PATHS = self.determine_general_s3_filepaths()
        self.s3 = s3
        self.Discord = DiscordAlert(self.diff_pairs)
        self.ArbDiff = ArbDiff(self.diff_pairs, self.market)

    # =============================================================================
    # Get market data for exchanges, iterate infinitely
    # =============================================================================
    def main(self):
        self.reset_for_new_day()
        sleep_to_desired_interval(self.interval)
        # while True:
        for i in range(20):
            # if determine_if_new_day(self.midnight):
            if i == 19:
                self.handle_midnight_event()
            self.get_bid_ask_and_process_df_and_test_diff()
            sleep_to_desired_interval(self.interval)

    # =============================================================================
    # It's midnight! Save important data and reset for next day
    # =============================================================================
    def handle_midnight_event(self):
        self.save_updated_data_to_s3()
        self.ArbDiff.main(self.df_obj, self.today)
        self.reset_for_new_day()  # must come last!

    # =============================================================================
    # Get bid ask data and update dataframe obj for all exchanges
    # =============================================================================
    def get_bid_ask_and_process_df_and_test_diff(self) -> dict:
        bid_asks = self.get_bid_ask_from_exchanges()
        self.update_df_obj_with_new_bid_ask_data(bid_asks)
        self.Discord.determine_exchange_diff_and_alert_discord(bid_asks)
        jprint(self.df_obj)

    # =============================================================================
    # Get current bid ask data from exchange using THREADDING
    # =============================================================================
    def get_bid_ask_from_exchanges(self) -> dict:
        bid_asks = {}
        now = determine_cur_utc_timestamp()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = executor.map(
                self.get_bid_ask_from_specific_exchange,
                self.exchanges_obj.items(),
                repeat(now),
            )
            for r in result:
                exchange, bid_ask = r
                bid_asks[exchange] = bid_ask
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

        bid_ask["timestamp"] = now
        bid_ask["mid"] = self.compute_mid(bid_ask)
        return exchange, bid_ask

    # =============================================================================
    # Get bid/ask market data for Ftx_Us
    # =============================================================================
    def get_bid_ask_ftx(self, market: str) -> dict:
        try:
            url = f"{FTX_BASEURL}{market}/orderbook"
            res = requests.get(url).json()["result"]
            bid_ask = self.pull_best_bid_ask_from_orderbook(res, "FTX_US")
        except Exception as e:
            print(f"Exception raised in get_bid_ask_ftx(): {e}")
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
            print(f"Exception raised in get_bid_ask_dydx(): {e}")
            bid_ask = self.create_nan_bid_ask_dict()
        return bid_ask

    # =============================================================================
    # Pull best bid/ask for DyDx, verify it's sorted correctly
    # =============================================================================
    def pull_best_bid_ask_from_orderbook(self, res: list, exchange: str) -> tuple:
        asks, bids = res["asks"], res["bids"]
        if exchange == "DYDX":
            asks = pd.DataFrame(asks)
            asks["price"] = pd.to_numeric(asks["price"])
            asks["size"] = pd.to_numeric(asks["size"])
            bids = pd.DataFrame(bids)
            bids["price"] = pd.to_numeric(bids["price"])
            bids["size"] = pd.to_numeric(bids["size"])

        if exchange == "FTX_US":
            asks = pd.DataFrame(asks, columns=["price", "size"])
            bids = pd.DataFrame(bids, columns=["price", "size"])

        best_ask = asks.iloc[asks["price"].idxmin()]
        best_bid = bids.iloc[bids["price"].idxmax()]
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
        if bid_ask["ask_price"] != asks.iloc[0]["price"]:
            print(f"{exchange} order book messed up: \n {asks}")
            raise Exception(f"{exchange} order book messed up: \n {asks}")
        if bid_ask["bid_price"] != float(bids.iloc[0]["price"]):
            print(f"{exchange} order book messed up: \n {bids}")
            raise Exception(f"{exchange} order book messed up: \n {bids}")

        diff = (bid_ask["ask_price"] / bid_ask["bid_price"] - 1) * 100
        if diff > 5:
            print(f"Warning, bid_ask diff is larger than 5%: {diff}.")
            print(f"This is unrelated to inter-exchange difference.")

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
    def update_df_obj_with_new_bid_ask_data(self, bid_asks: dict) -> dict:
        for exchange, bid_ask in bid_asks.items():
            if exchange not in self.df_obj:
                df = self.create_new_df_with_bid_ask(bid_ask)
            else:
                cur_df = self.df_obj[exchange]
                df = self.append_existing_df_with_bid_ask(bid_ask, cur_df)
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
    # Save the updated df to S3
    # =============================================================================
    def save_updated_data_to_s3(self) -> None:
        for exchange, df in self.df_obj.items():
            df = self.prepare_df_for_s3(df)
            path = self.update_cur_s3_filepath(self.S3_BASE_PATHS[exchange])
            print(path)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            response = s3.put_object(
                Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
            )
            jprint(response)

    # =============================================================================
    # Preare the final df_obj to be save to S3
    # =============================================================================
    def prepare_df_for_s3(self, df) -> dict:
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index).tz_localize(None)
        return df

    # =============================================================================
    # Create filesnames for today's date (date in filename!)
    # =============================================================================
    def update_cur_s3_filepath(self, base_path: str):
        return f"{base_path}-{self.today}.csv"

    # =============================================================================
    #
    # HELPERS
    #
    # =============================================================================

    # =============================================================================
    # Create all unique exchange pairs for diff calculation later
    # =============================================================================
    def create_unique_exchange_pairs(self):
        i = 0
        pairs = []
        for i, ex in enumerate(self.exchanges[:-1]):
            j = i + 1
            for ex2 in self.exchanges[j:]:
                pairs.append(f"{ex}-{ex2}")
        return pairs

    # =============================================================================
    # Get exchanges, make sure they're not hyphonated
    # =============================================================================
    def make_list_of_exchanges(self, exchanges_obj: dict):
        exchanges = list(exchanges_obj.keys())
        for ex in exchanges:
            if "-" in ex:
                msg = f"Naming convention: Rename exchange. {ex} cannot contain a - (hyphon) in its name."
                raise Exception(msg)
        return exchanges

    # =============================================================================
    # Determine market and make uniform (e.g. ETH/USD => ETH-USD)
    # =============================================================================
    def determine_market(self):
        symbols = []
        for market in self.exchanges_obj.values():
            if "/" in market:
                market = market.replace("/", "-")
            elif "_" in market:
                market = market.replace("_", "-")
            symbols.append(market)
        if all([x == symbols[0] for x in symbols]):
            return symbols[0]
        raise Exception(f"Inproperly formatted market. {symbols}")

    # =============================================================================
    # Get all relevant filepaths to fetch and save data to
    # =============================================================================
    def determine_general_s3_filepaths(self) -> dict:
        s3_base_paths = {}
        for exchange in self.exchanges_obj.keys():
            path = f"{exchange}/{self.market}/{exchange}-{self.market}"
            s3_base_paths[exchange] = path
        return s3_base_paths

    # =============================================================================
    # Reset all values that start a new with new day
    # =============================================================================
    def reset_for_new_day(self) -> tuple:
        self.today = determine_today_str_timestamp()
        self.midnight = determine_next_midnight()
        self.df_obj = {}


if __name__ == "__main__":
    # to active venv: source venv/bin/activate
    exchanges_obj = {"FTX_US": "ETH/USD", "DYDX": "ETH-USD"}
    obj = ArbDataPuller(exchanges_obj=exchanges_obj, interval=5)
    obj.main()
