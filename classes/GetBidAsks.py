# =============================================================================
# IMPORTS
# =============================================================================
import os, sys, json
import datetime as dt
from dotenv import load_dotenv
import requests
import dydx3
import pandas as pd
import numpy as np


# =============================================================================
# FILE IMPORTS
# =============================================================================
load_dotenv()
sys.path.append(os.path.abspath("./utils"))
from utils.jprint import jprint
from utils.constants import FTX_BASEURL, DYDX_BASEURL, BUCKET_NAME


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
# Pull bid/ask from exchanges, save to S3 at midnight
# =============================================================================
class GetBidAsks:
    def __init__(self, market):
        self.market = market

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
            client = dydx3.Client(host=DYDX_BASEURL)
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
