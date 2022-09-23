# =============================================================================
# IMPORTS
# =============================================================================
import traceback
from discord import SyncWebhook
from utils.decimal_helper import dec
from utils.jprint import jprint
from utils.time_helpers import (
    determine_cur_utc_timestamp,
    convert_datetime_str_to_obj,
    convert_sec_to_min,
)


# =============================================================================
# CONSTANTS
# =============================================================================
DISCORD_URL = "https://discord.com/api/webhooks/1022260697037541457/sH6v5xoDBSykaEyn1W91GtesMVC6PurG8ksESCbwR5VlxXi9FXFWlrc-OmnHQzA7RBWN"
SECS_PER_HOUR = 60 * 60

# =============================================================================
# This class check bid_ask data for exchange arbitrage opportunities and send to Discord
# =============================================================================
class DiscordAlert:
    def __init__(self, diff_pairs):
        self.diff_pairs = diff_pairs
        self.thresholds = {p: {"value": 1, "timestamp": None} for p in diff_pairs}
        self.thresh_reset_time = SECS_PER_HOUR
        self.thresh_incr = 5  # $$$-terms used to upwards increment thresh

    # =============================================================================
    # Check $$$ diff between exchanges and alert discord if sufficient.
    # =============================================================================
    def determine_exchange_diff_and_alert_discord(self, bid_asks: dict):
        try:
            msgs = self.determine_exchange_diff(bid_asks)
            print("Discord: ", msgs)
            # self.post_msg_to_discord(msgs)
        except Exception as e:
            traceback.print_exc()
            print(f"Disord webhook failed with this message: {e}")

    # =============================================================================
    # Check $$$ diff between exchanges
    # =============================================================================
    def determine_exchange_diff(self, bid_asks: list):
        msgs = []
        for pair in self.diff_pairs:
            ex0, ex1 = pair.split("-")
            bid_ask0, bid_ask1 = bid_asks[ex0], bid_asks[ex1]
            diff = self.compute_price_diff(bid_ask0, bid_ask1)
            cur_thresh = self.check_thresh_and_reset_if_necessary(pair)
            if cur_thresh["value"] < diff:
                msg = self.format_msg_for_discord(pair, diff)
                msgs.append(msg)
                self.increase_and_update_threshold(pair)
        return msgs

    # =============================================================================
    # Compute difference between prices, correctly formatted and rounded
    # =============================================================================
    def compute_price_diff(self, bid_ask0: dict, bid_ask1: dict):
        mid0, mid1 = bid_ask0["mid"], bid_ask1["mid"]
        diff = abs(dec(mid0) - dec(mid1))
        return float(diff)

    # =============================================================================
    # Check current thresh and reset if time is up
    # =============================================================================
    def check_thresh_and_reset_if_necessary(self, pair):
        last_triggered = self.thresholds[pair]["timestamp"]
        if last_triggered is None:
            return self.thresholds[pair]
        now = determine_cur_utc_timestamp()
        secs_apart = (now - last_triggered).seconds
        if secs_apart > self.thresh_reset_time:
            self.reset_thresold(pair)
        return self.thresholds[pair]

    # =============================================================================
    # Check current thresh and reset if time is up
    # =============================================================================
    def increase_and_update_threshold(self, pair):
        val = self.thresholds[pair]["value"]
        new_val = (val // self.thresh_incr) * self.thresh_incr + self.thresh_incr
        new_timestamp = determine_cur_utc_timestamp()
        self.thresholds[pair] = {"value": new_val, "timestamp": new_timestamp}

    # =============================================================================
    # Format message for discord webhook
    # =============================================================================
    def format_msg_for_discord(self, pair: str, diff: float):
        thresh_val = self.thresholds[pair]["value"]
        msg0 = f"ALERT (semi-testing): Arbitrage opportunity.\n"
        msg1 = f"Diff surpassed threshold of: ${thresh_val}\n"
        msg2 = f"Price difference between {pair} is: ${diff}"
        return msg0 + msg1 + msg2

    # =============================================================================
    # Post messages to discord
    # =============================================================================
    def post_msg_to_discord(self, msgs):
        webhook = SyncWebhook.from_url(DISCORD_URL)
        if type(msgs) is str:
            webhook.send(msgs)
        elif type(msgs) is list:
            for msg in msgs:
                webhook.send(msg)

    # =============================================================================
    # Reset thresholds
    # =============================================================================
    def reset_thresold(self, pair):
        self.thresholds[pair] = {"value": 1, "timestamp": None}


if __name__ == "__main__":
    disc = DiscordAlert(["DYDX-FTX_US"])
    bid_asks = [
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
        {"DYDX": {"mid": 15}, "FTX_US": {"mid": 10}},
    ]
    for b_a in bid_asks:
        disc.determine_exchange_diff_and_alert_discord(b_a)
