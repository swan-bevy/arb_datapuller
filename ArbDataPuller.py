# =============================================================================
# IMPORTS
# =============================================================================
import os, sys, json
import boto3
from io import StringIO
from dotenv import load_dotenv
import pandas as pd
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
from utils.constants import BUCKET_NAME
from classes.GetBidAsks import GetBidAsks
from classes.DiscordAlert import DiscordAlert
from classes.EodDiff import EodDiff
from classes.SaveRawData import SaveRawData
from classes.FrozenOrderbook import FrozenOrderbook

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
# Pull bid/ask from exchanges, save to S3 at midnight
# =============================================================================
class ArbDataPuller:
    def __init__(self, market: str, exchanges_obj: dict):
        self.market = self.check_market(market)
        self.exchanges_obj = exchanges_obj
        self.interval = self.ask_user_for_interval()

        self.exchanges = self.make_list_of_exchanges(exchanges_obj)
        self.diff_pairs = self.create_unique_exchange_pairs()
        self.S3_BASE_PATHS = self.determine_general_s3_filepaths()
        self.s3 = s3

        self.GetBidAsks = GetBidAsks()
        self.FrozenOrderbook = FrozenOrderbook(self)
        self.Discord = DiscordAlert(self)
        self.SaveRawData = SaveRawData(self)
        self.EodDiff = EodDiff(self)

    # =============================================================================
    # Get market data for exchanges, iterate infinitely
    # =============================================================================
    def main(self):
        print("MAKE SURE THRESHS ARE APPROPRIATE!")
        self.reset_for_new_day()
        sleep_to_desired_interval(self.interval)
        while True:
            if determine_if_new_day(self.midnight):
                self.handle_midnight_event()
            self.get_bid_ask_and_process_df_and_test_diff()
            sleep_to_desired_interval(self.interval)

    # =============================================================================
    # It's midnight! Save important data and reset for next day
    # =============================================================================
    def handle_midnight_event(self):
        self.SaveRawData.save_raw_bid_ask_data_to_s3()
        self.EodDiff.determine_eod_diff_n_create_summary(self.df_obj, self.today)
        self.reset_for_new_day()  # must come last!

    # =============================================================================
    # Get bid ask data and update dataframe obj for all exchanges
    # =============================================================================
    def get_bid_ask_and_process_df_and_test_diff(self) -> dict:
        bid_asks = self.get_bid_ask_from_exchanges()
        self.update_df_obj_with_new_bid_ask_data(bid_asks)
        self.FrozenOrderbook.check_all_orderbooks_if_frozen()
        self.Discord.determine_exchange_diff_and_alert_discord(bid_asks)
        print("=========================================\n")
        jprint(self.df_obj)

    # =============================================================================
    # Get current bid ask data from exchange using THREADDING
    # =============================================================================
    def get_bid_ask_from_exchanges(self) -> dict:
        bid_asks = {}
        now = determine_cur_utc_timestamp()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = executor.map(
                self.GetBidAsks.get_bid_ask_from_specific_exchange,
                self.exchanges_obj.items(),
                repeat(now),
            )
            for r in result:
                exchange, bid_ask = r
                bid_asks[exchange] = bid_ask
        return bid_asks

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
    def reset_for_new_day(self):
        self.today = determine_today_str_timestamp()
        self.midnight = determine_next_midnight()
        self.df_obj = {}

    # =============================================================================
    # Ask user for interval on how often to fetch bid/ask
    # =============================================================================
    def ask_user_for_interval(self):
        inp = int(input("Specify the desired interval in seconds: ").strip())
        if inp < 5:
            raise ValueError("Interval is too small. Execution cancelled.")
        return inp

    # =============================================================================
    # Make sure standardized market input has a valid format
    # =============================================================================
    def check_market(self, market):
        if "-" not in market or not market.isupper():
            raise ValueError("Invalid market format. Should be like so: `BTC-USD`.")
        return market


if __name__ == "__main__":
    # to activate EC2: ssh -i "ec2-arb-stats.pem" ec2-user@ec2-3-120-243-216.eu-central-1.compute.amazonaws.com
    # to active venv: source venv/bin/activate
    # BTC-USD '{"DYDX": "BTC-USD", "OKX": "BTC-USDT", "BINANCE_US": "BTCUSD"}'
    # ETH-USD '{"DYDX": "ETH-USD", "OKX": "ETH-USDT"}'
    # SOL-USD '{"DYDX": "SOL-USD", "OKX": "SOL-USDT"}'
    # UNI-USD '{"DYDX": "UNI-USD", "OKX": "UNI-USDT"}'
    # LTC-USD '{"DYDX": "LTC-USD", "OKX": "LTC-USDT"}'
    if len(sys.argv) < 3:
        raise Exception(
            'Need to enter exchanges dict like so: \'{"FTX_US": "BTC/USD", "DYDX": "BTC-USD"}\''
        )
    market = sys.argv[1]
    exchanges_obj = json.loads(sys.argv[2])
    obj = ArbDataPuller(market=market, exchanges_obj=exchanges_obj)
    obj.main()
