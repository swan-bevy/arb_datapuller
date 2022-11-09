# =============================================================================
# IMPORTS
# =============================================================================
import sys, os
from io import StringIO
import pandas as pd


# =============================================================================
# FILE IMPORTS
# =============================================================================
sys.path.append(os.path.abspath("./utils"))
from utils.jprint import jprint
from utils.constants import BUCKET_NAME, S3

# =============================================================================
# Save raw exchange specific data to S3
# =============================================================================
class SaveRawData:
    def __init__(self, Caller):
        self.Caller = Caller

    # =============================================================================
    # Save the updated df to S3
    # =============================================================================
    def save_raw_bid_ask_data_to_s3(self) -> None:
        for exchange, df in self.Caller.df_obj.items():
            df = self.prepare_df_for_s3(df)
            path = self.update_cur_s3_filepath(self.Caller.S3_BASE_PATHS[exchange])
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            response = S3.put_object(
                Bucket=BUCKET_NAME, Key=path, Body=csv_buffer.getvalue()
            )
            print(
                f"{path} saved with status code: {response['ResponseMetadata']['HTTPStatusCode']}"
            )

    # =============================================================================
    # Preare the final df_obj to be save to S3
    # =============================================================================
    def prepare_df_for_s3(self, df) -> dict:
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df = df[["bid_price", "ask_price", "bid_size", "ask_size", "mid"]]
        return df

    # =============================================================================
    # Create filesnames for today's date (date in filename!)
    # =============================================================================
    def update_cur_s3_filepath(self, base_path: str):
        return f"{base_path}-{self.Caller.today}.csv"
