# =============================================================================
# IMPORTS
# =============================================================================
from cmath import pi
import pandas as pd
import os, traceback
import boto3
import glob
from io import StringIO
from pprint import pprint
from utils.jprint import jprint
from dotenv import load_dotenv
from utils.discord_hook import post_msgs_to_discord
from utils.constants import BUCKET_NAME, DISCORD_URL

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
# ISSUES:
#   1. The way I determine date for the filepath for S3 is not great
#   2. I don't like to have to functions for saving to S3, in ArbDataPuller & ArbDiff
# =============================================================================

# =============================================================================
# Determine bid/ask differences between exchanges
# CAUTION: Differentiate between (original) df_obj & (processed) merged_obj!!!
# =============================================================================
class EodDiff:
    def __init__(self, diff_pairs, market, interval):
        self.diff_pairs = diff_pairs
        self.market = market
        self.interval = interval

    # =============================================================================
    # Compute diffs between exchanges, save to S3 and send summary to Discord
    # =============================================================================
    def determine_eod_diff_n_create_summary(self, df_obj: dict, today: str):
        try:
            self.merge_dfs_for_pairs(df_obj)
            self.compute_price_diffs()
            self.reorder_df_columns()
            self.prepare_diff_dfs_for_s3()
            self.save_diff_dfs_to_s3(today)
            self.create_n_send_summary_to_discord(today)
        except Exception as e:
            traceback.print_exc()
            print(f"ArbDiff failed execution with error message: {e}")

    # =============================================================================
    # Create merged dfs for exchange pairs
    # =============================================================================
    def merge_dfs_for_pairs(self, df_obj):
        merged_obj = {}
        for pair in self.diff_pairs:
            ex0, ex1 = pair.split("-")
            df0, df1 = df_obj[ex0], df_obj[ex1]
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
            df[f"{pair}_ask"] = (df[f"{ex0}_ask"] - df[f"{ex1}_ask"]).abs().round(2)
            df[f"{pair}_bid"] = (df[f"{ex0}_bid"] - df[f"{ex1}_bid"]).abs().round(2)
            df[f"{pair}_mid"] = (df[f"{ex0}_mid"] - df[f"{ex1}_mid"]).abs().round(2)
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
    # Format timestamps and such for MERGED OBJ
    # =============================================================================
    def prepare_diff_dfs_for_s3(self):
        for pair, df in self.merged_obj.items():
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp")
            self.merged_obj[pair] = df

    # =============================================================================
    # Format timestamps and such
    # =============================================================================
    def save_diff_dfs_to_s3(self, today):
        base = f"Difference/{self.market}/{today}"
        for ex_pair, df in self.merged_obj.items():
            path = f"{base}/{ex_pair}_{self.market}_{today}.csv"
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            response = s3.put_object(
                Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
            )
            print(
                f"{path} saved with status code: {response['ResponseMetadata']['HTTPStatusCode']}"
            )

    # =============================================================================
    # Make message and send to discord
    # =============================================================================
    def create_n_send_summary_to_discord(self, today):
        self.msg = f"End of day: {today} UTC.\n"
        date = today.split(" ")[0]
        for pair, df in self.merged_obj.items():
            info = self.determine_eod_vals(date, pair, df)
            self.format_msg_for_discord(info)
        post_msgs_to_discord(DISCORD_URL, self.msg)

    # =============================================================================
    # Determine vals like max, min, mean for EOD summary
    # =============================================================================
    def determine_eod_vals(self, date: str, pair: str, df: pd.DataFrame):
        ex0, ex1 = pair.split("-")
        diff_col = f"{pair}_mid"
        info = {"pair": pair, "date": date}

        _max = df.loc[df[diff_col].idxmax()]
        info["max_abs"] = _max[diff_col]
        info["max_perc"] = self.compute_perc_diff(diff_col, _max, ex0, ex1)

        _min = df.loc[df[diff_col].idxmin()]
        info["min_abs"] = _min[diff_col]
        info["min_perc"] = self.compute_perc_diff(diff_col, _min, ex0, ex1)

        info["mean_abs"] = round(df[diff_col].mean(), 2)
        return info

    # =============================================================================
    # Format msg with info
    # =============================================================================
    def format_msg_for_discord(self, info):
        ex0, ex1 = info["pair"].split("-")

        msg1 = f"{ex0} & {ex1} trading {self.market} at interval {self.interval} seconds:\n"

        msg2 = f" - Max diff absolute: ${info['max_abs']}\n"
        msg3 = f" - Max diff percentage: {info['max_perc']}%\n"

        msg4 = f" - Min diff absolute: ${info['min_abs']}\n"
        msg5 = f" - Min diff percentage: {info['min_perc']}%\n"

        msg6 = f" - Mean diff absolute: ${info['mean_abs']}\n"
        msg_div = f"\n=================================\n\n"
        pair_msg = msg_div + msg1 + msg2 + msg3 + msg4 + msg5 + msg6
        self.msg += pair_msg

    # =============================================================================
    # Compute mean on mid prices
    # =============================================================================
    def compute_perc_diff(self, diff_col, row, ex0, ex1):
        diff_abs = row[diff_col]
        _mean = (row[f"{ex0}_mid"] + row[f"{ex1}_mid"]) / 2
        return round(diff_abs / _mean * 100, 3)
