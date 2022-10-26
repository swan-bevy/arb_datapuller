# =============================================================================
# IMPORTS
# =============================================================================
import os, sys, json
import datetime as dt
from dotenv import load_dotenv
import requests
import dydx3
from binance.spot import Spot as BinanceSpot
import pandas as pd
import numpy as np
import traceback

# =============================================================================
# LOGGING
# =============================================================================
import logging

logging.basicConfig(
    format="%(asctime)s - %(message)s", filename="logs/exchange_errors.log"
)


# =============================================================================
# FILE IMPORTS
# =============================================================================
load_dotenv()
sys.path.append(os.path.abspath("./utils"))
from utils.jprint import jprint
from utils.constants import FTX_BASEURL, DYDX_BASEURL, OKX_BASEURL, BUCKET_NAME


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
    def __init__(self):
        self.dydx_client = dydx3.Client(host=DYDX_BASEURL)
        self.binance_client = BinanceSpot()

    # =============================================================================
    # Determine the exchange and run function
    # =============================================================================
    def get_bid_ask_from_specific_exchange(
        self, exchange_and_market: tuple, now: dt
    ) -> dict:
        exchange, market = exchange_and_market[0], exchange_and_market[1]
        try:
            bid_ask = self.determine_exch_n_get_data(exchange, market)
        except Exception as e:
            logging.error(
                f"Exception occurred with {market} at {exchange}", exc_info=True
            )

            self.log_exception(exchange, e)
            bid_ask = self.create_nan_bid_ask_dict()

        bid_ask["timestamp"] = now
        bid_ask["mid"] = self.compute_mid(bid_ask)
        return exchange, bid_ask

    # =============================================================================
    # Determine which exchange, and fetch data
    # =============================================================================
    def determine_exch_n_get_data(self, exchange, market):
        if exchange == "FTX_US":
            res = self.get_bid_ask_ftx_us(market)
        elif exchange == "DYDX":
            res = self.get_bid_ask_dydx(market)
        elif exchange == "BINANCE":
            res = self.get_bid_ask_binance(market)
        elif exchange == "OKX":
            res = self.get_bid_ask_okx(market)
        else:
            raise Exception("No function exists for this exchange.")
        bid_ask = self.process_orderbook_res_from_ex(res, exchange)
        return bid_ask

    # =============================================================================
    # Get bid/ask market data for Ftx_Us
    # =============================================================================
    def get_bid_ask_ftx_us(self, market: str) -> dict:
        url = f"{FTX_BASEURL}{market}/orderbook"
        return requests.get(url).json()["result"]

    # =============================================================================
    # Get bid/ask market data for DyDx
    # =============================================================================
    def get_bid_ask_dydx(self, market: str) -> dict:
        return self.dydx_client.public.get_orderbook(market=market).data

    # =============================================================================
    # Get bid/ask market data for Binance
    # =============================================================================
    def get_bid_ask_binance(self, market: str) -> dict:
        return self.binance_client.depth(symbol=market)

    # =============================================================================
    # Get bid/ask market data for OkX
    # =============================================================================
    def get_bid_ask_okx(self, market: str) -> dict:
        url = f"{OKX_BASEURL}api/v5/market/books?instId={market}&sz=5"
        res = requests.get(url).json()["data"]
        if len(res) > 1:
            raise Exception(f"OKX returned more than one orderbook: {res}")
        res = res[0]
        res["asks"] = [r[0:2] for r in res["asks"]]
        res["bids"] = [r[0:2] for r in res["bids"]]
        return res

    # =============================================================================
    # Pull best bid/ask for DyDx, verify it's sorted correctly
    # =============================================================================
    def process_orderbook_res_from_ex(self, res: list, exchange: str) -> tuple:
        asks, bids = self.convert_orderbook_to_df(res, exchange)

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
    # Pull data out of res and process to dataframe considering exchange specifics
    # =============================================================================
    def convert_orderbook_to_df(self, res, exchange):
        asks, bids = res["asks"], res["bids"]
        if exchange == "DYDX":
            asks = pd.DataFrame(asks)
            bids = pd.DataFrame(bids)
        elif exchange in ["FTX_US", "BINANCE"]:
            asks = pd.DataFrame(asks, columns=["price", "size"])
            bids = pd.DataFrame(bids, columns=["price", "size"])
        elif exchange == "OKX":
            asks = pd.DataFrame(asks, columns=["price", "size"])
            bids = pd.DataFrame(bids, columns=["price", "size"])
        asks = self.convert_column_to_numeric(asks)
        bids = self.convert_column_to_numeric(bids)
        return asks, bids

    # =============================================================================
    # Some exchanges return strings, convert to numeric
    # =============================================================================
    def convert_column_to_numeric(self, df):
        df["price"] = pd.to_numeric(df["price"])
        df["size"] = pd.to_numeric(df["size"])
        return df

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
    # Log exception to concole when data fetching from exchange fails
    # =============================================================================
    def log_exception(self, exchange, err):
        print("\n\n")
        traceback.print_exc()
        print(f"\n\nError getting data for {exchange}: {err}\n\n")

    # =============================================================================
    # Compute mid between ask and bid_ask
    # =============================================================================
    def compute_mid(self, bid_ask: dict) -> float:
        mid = (bid_ask["ask_price"] + bid_ask["bid_price"]) / 2
        return round(mid, 3)
