# =============================================================================
# IMPORTS
# =============================================================================
import os, sys, time
import datetime as dt
from dotenv import load_dotenv
import requests
import dydx3
import pandas as pd
import numpy as np
import traceback

from sympy import E

# =============================================================================
# FILE IMPORTS
# =============================================================================
load_dotenv()
sys.path.append(os.path.abspath("./utils"))
from utils.jprint import jprint
from utils.constants import (
    DYDX_BASEURL,
    OKX_BASEURL,
    BINANCE_US_BASEURL,
    BINANCE_GLOBAL_BASEURL,
    COINBASE_BASEURL,
)
from utils.logger import get_logger

log = get_logger()


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
    MAX_RETRIES = 5  # max amount of time to retry fetching from the exchange

    def __init__(self, Caller):
        self.Caller = Caller
        self.dydx_client = dydx3.Client(host=DYDX_BASEURL)

    # =============================================================================
    # Determine the exchange and run function
    # =============================================================================
    def get_bid_ask_from_specific_exchange(self, exchange_n_market: tuple, now) -> dict:
        exchange, market = exchange_n_market[0], exchange_n_market[1]
        bid_ask = self.get_bid_ask_n_error_check(exchange, market)
        bid_ask["timestamp"] = now
        bid_ask["mid"] = self.compute_mid(bid_ask)
        return exchange, bid_ask

    # =============================================================================
    # Get bid ask from exchage, error check and refetch if messed up
    # =============================================================================
    def get_bid_ask_n_error_check(self, exchange, market) -> dict:
        count = 0
        while True:
            try:
                res = self.determine_exch_n_get_data(exchange, market)
                return self.process_n_error_check_res(res, exchange)
            except Exception as e:
                log.exception(e)
                self.print_exception(exchange, e)
                if count >= self.MAX_RETRIES:
                    return self.create_nan_bid_ask_dict()
                count += 1
                time.sleep(0.1)

    # =============================================================================
    # Determine which exchange, and fetch data
    # =============================================================================
    def determine_exch_n_get_data(self, exchange, market):
        if exchange == "DYDX":
            return self.get_bid_ask_dydx(market)
        elif exchange == "BINANCE_US":
            return self.get_bid_ask_binance_us(market)
        elif exchange == "BINANCE_GLOBAL":
            return self.get_bid_ask_binance_global(market)
        elif exchange == "OKX":
            return self.get_bid_ask_okx(market)
        elif exchange == "COINBASE":
            return self.get_bid_ask_coinbase(market)
        else:
            raise Exception("No function exists for this exchange.")

    # =============================================================================
    # Get bid/ask market data for DyDx
    # =============================================================================
    def get_bid_ask_dydx(self, market: str) -> dict:
        res = requests.get(f"{DYDX_BASEURL}/orderbook/{market}")
        return res.json()

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
    # Pull best bid/ask from Binance US
    # =============================================================================
    def get_bid_ask_binance_us(self, market):
        res = requests.get(BINANCE_US_BASEURL + f"symbol={market}")
        return res.json()

    # =============================================================================
    # Pull best bid/ask from Binance Global
    # =============================================================================
    def get_bid_ask_binance_global(self, market):
        res = requests.get(BINANCE_GLOBAL_BASEURL + f"/depth?symbol={market}&limit=10")
        return res.json()

    # =============================================================================
    # Pull best bid/ask from CoinBase
    # =============================================================================
    def get_bid_ask_coinbase(self, market):
        url = f"{COINBASE_BASEURL}{market}/book?level=1"
        res = requests.get(url, headers={"accept": "application/json"}).json()
        print("Make sure Coinbase is alright!")
        return {"bids": [res["bids"][0][0:2]], "asks": [res["asks"][0][0:2]]}

    # =============================================================================
    # Pull best bid/ask for DyDx, verify it's sorted correctly
    # =============================================================================
    def process_n_error_check_res(self, res: list, exchange: str) -> tuple:
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
        if exchange in ["DYDX"]:
            asks = pd.DataFrame(asks)
            bids = pd.DataFrame(bids)
        elif exchange in [
            "FTX_US",
            "FTX_GLOBAL",
            "BINANCE_US",
            "BINANCE_GLOBAL",
            "OKX",
            "COINBASE",
        ]:
            asks = pd.DataFrame(asks, columns=["price", "size"])
            bids = pd.DataFrame(bids, columns=["price", "size"])
        else:
            raise Exception(f"Not implemented for exchange {exchange}")
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
            raise Exception(f"{exchange} orderbook messed up: \n {asks}")
        if bid_ask["bid_price"] != float(bids.iloc[0]["price"]):
            print(f"{exchange} order book messed up: \n {bids}")
            raise Exception(f"{exchange} orderbook messed up: \n {bids}")

        diff = self.determine_bid_ask_diff(bid_ask)
        if diff >= 0.09:
            raise Exception(f"{exchange} orderbook is lose: {bid_ask}")

    # =============================================================================
    # Determine tick difference between bid, ask, to make sure orderbook is proper
    # =============================================================================
    def determine_bid_ask_diff(self, bid_ask):
        ask, bid = bid_ask["ask_price"], bid_ask["bid_price"]
        mid = self.compute_mid(bid_ask)
        diff = ((ask - bid) / mid) * 100
        return abs(diff)

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
    def print_exception(self, exchange, err):
        print("\n\n")
        traceback.print_exc()
        print(f"\n\nError getting data for {exchange}: {err}\n\n")

    # =============================================================================
    # Compute mid between ask and bid_ask
    # =============================================================================
    def compute_mid(self, bid_ask: dict) -> float:
        mid = (bid_ask["ask_price"] + bid_ask["bid_price"]) / 2
        return round(mid, 6)
