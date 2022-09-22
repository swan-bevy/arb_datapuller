# =============================================================================
# IMPORTS
# =============================================================================
import pandas as pd
import datetime as dt
import sys, os
import boto3
import glob
from io import StringIO
from pprint import pprint
from utils.time_helpers import convert_timestamp_to_today_date
from dotenv import load_dotenv

load_dotenv()

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
INPUT_PATH = "/Users/julian/Documents/arb_datapuller/diff_data"  # delete
FILES = glob.glob(os.path.join(INPUT_PATH, "*.csv"))  # delete
BUCKET_NAME = "arb-live-data"

# =============================================================================
# ISSUES:
#   1. The way I determine date for the filepath for S3 is not great
#   2. I don't like to have to functions for saving to S3, in ArbDataPuller & ArbDiff
# =============================================================================

# =============================================================================
# Determine bid/ask differences between exchanges
# =============================================================================
class ArbDiff:
    def __init__(self, df_obj: dict):
        self.df_obj = df_obj
        self.exchanges = list(df_obj.keys())
        self.pairs = self.create_unique_exchange_pairs()

    # =============================================================================
    # bla bla
    # =============================================================================
    def main(self):
        self.merge_dfs_for_pais()
        self.compute_price_diffs()
        self.reorder_df_columns()
        self.prepare_diff_dfs_for_s3()
        self.save_diff_dfs_to_s3()

    # =============================================================================
    # Create all unique exchange pairs
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
    # Create merged dfs for exchange pairs
    # =============================================================================
    def merge_dfs_for_pais(self):
        merged_obj = {}
        for pair in self.pairs:
            ex0, ex1 = pair.split("-")
            df0, df1 = self.df_obj[ex0], self.df_obj[ex1]
            df0 = self.rename_columns(ex0, df0)
            df1 = self.rename_columns(ex1, df1)
            merged = pd.merge(df0, df1, on="timestamp", how="inner")
            merged_obj[pair] = merged
        self.merged_obj = merged_obj

    # =============================================================================
    # Rename columns to include exchange in column name
    # =============================================================================
    def rename_columns(self, ex, df) -> pd.DataFrame:
        df = df[["timestamp", "ask_price", "bid_price", "mid"]]
        rename_dict = {
            "ask_price": f"{ex}_ask",
            "bid_price": f"{ex}_bid",
            "mid": f"{ex}_mid",
        }
        return df.rename(columns=rename_dict)

    # =============================================================================
    # Compute diff between exchanges
    # =============================================================================
    def compute_price_diffs(self):
        for pair, df in self.merged_obj.items():
            ex0, ex1 = pair.split("-")
            df[f"{pair}_ask"] = (df[f"{ex0}_ask"] - df[f"{ex1}_ask"]).abs()
            df[f"{pair}_bid"] = (df[f"{ex0}_bid"] - df[f"{ex1}_bid"]).abs()
            df[f"{pair}_mid"] = (df[f"{ex0}_mid"] - df[f"{ex1}_mid"]).abs()
            self.merged_obj[pair] = df

    # =============================================================================
    # Reorder columns to have ex0_ask, ex1_ask, ask_diff => side-by-side
    # =============================================================================
    def reorder_df_columns(self):
        for pair, df in self.merged_obj.items():
            ex0, ex1 = pair.split("-")
            df = df[
                [
                    "timestamp",
                    f"{ex0}_ask",
                    f"{ex1}_ask",
                    f"{pair}_ask",
                    f"{ex0}_bid",
                    f"{ex1}_bid",
                    f"{pair}_bid",
                    f"{ex0}_mid",
                    f"{ex1}_mid",
                    f"{pair}_mid",
                ]
            ]
            self.merged_obj[pair] = df

    # =============================================================================
    # Format timestamps and such
    # =============================================================================
    def prepare_diff_dfs_for_s3(self):
        for pair, df in self.merged_obj.items():
            ### REMOVE WHEN USING THE REAL DATA ###
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp")
            self.merged_obj[pair] = df

    # =============================================================================
    # Format timestamps and such
    # =============================================================================
    def save_diff_dfs_to_s3(self):
        for pair, df in self.merged_obj.items():
            today = convert_timestamp_to_today_date(df.index[0])
            path = f"Difference/{today}/{pair}_{today}.csv"

            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            response = s3.put_object(
                Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
            )
            pprint(response)


if __name__ == "__main__":
    df_obj = {}
    for file in FILES:
        exchange = file.split("/")[-1].split("-")[0]
        df = pd.read_csv(file)
        df_obj[exchange] = df

    obj = ArbDiff(df_obj)
    obj.main()
