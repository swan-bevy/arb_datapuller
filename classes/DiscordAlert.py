# =============================================================================
# IMPORTS
# =============================================================================
import traceback
from discord import SyncWebhook
from utils.decimal_helper import dec
from utils.jprint import jprint
from utils.time_helpers import determine_cur_utc_timestamp
from utils.discord_hook import post_msgs_to_discord
from utils.constants import DISCORD_URL, SECS_PER_HOUR
from copy import deepcopy

# =============================================================================
# This class check bid_ask data for exchange arbitrage opportunities and send to Discord
# =============================================================================
class DiscordAlert:
    def __init__(self, diff_pairs, market, interval):
        self.diff_pairs = diff_pairs
        self.market = market
        self.interval = interval
        self.thresh_base = {"value": 0.2, "timestamp": None}
        self.thresholds = {p: deepcopy(self.thresh_base) for p in diff_pairs}
        self.thresh_reset_time = SECS_PER_HOUR
        self.thresh_incr = 0.1  # $$$-terms used to upwards increment thresh

    # =============================================================================
    # Check $$$ diff between exchanges and alert discord if sufficient.
    # =============================================================================
    def determine_exchange_diff_and_alert_discord(self, bid_asks: dict):
        try:
            msgs = self.determine_exchange_diff(bid_asks)
            if len(msgs) > 0:
                jprint(msgs)
                post_msgs_to_discord(DISCORD_URL, msgs)
        except Exception as e:
            traceback.print_exc()
            print(f"Disord webhook failed with this message: {e}")

    # =============================================================================
    # Check $$$ diff between exchanges
    # =============================================================================
    def determine_exchange_diff(self, bid_asks: list):
        msgs = []
        jprint("Thresh: ", self.thresholds)
        for pair in self.diff_pairs:
            ex0, ex1 = pair.split("-")
            bid_ask0, bid_ask1 = bid_asks[ex0], bid_asks[ex1]
            diff = self.compute_price_diff(bid_ask0, bid_ask1)
            cur_thresh = self.check_thresh_and_reset_if_necessary(pair)
            if cur_thresh["value"] < diff["pct"]:
                msg = self.format_msg_for_discord(pair, diff)
                msgs.append(msg)
                self.increase_and_update_threshold(pair)
        return msgs

    # =============================================================================
    # Compute difference between prices, correctly formatted and rounded
    # =============================================================================
    def compute_price_diff(self, bid_ask0: dict, bid_ask1: dict):
        mid0, mid1 = dec(bid_ask0["mid"]), dec(bid_ask1["mid"])
        abs_diff = abs(mid0 - mid1)
        avg = (mid0 + mid1) / 2
        pct_diff = abs_diff / avg * 100
        return {"abs": float(round(abs_diff, 2)), "pct": float(round(pct_diff, 2))}

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
        ex0, ex1 = pair.split("-")
        thresh_val = self.thresholds[pair]["value"]
        msg0 = f"ALERT: Arbitrage opportunity.\n"
        msg1 = f"{ex0} & {ex1} trading {self.market} at interval {self.interval} seconds:\n"
        msg2 = f"Diff surpassed % threshold of: {thresh_val}%\n"
        msg3 = f"Percentage price difference: {diff['pct']}%\n"
        msg4 = f"Absolute price difference: ${diff['abs']}"
        return msg0 + msg1 + msg2 + msg3 + msg4

    # =============================================================================
    # Reset thresholds
    # =============================================================================
    def reset_thresold(self, pair):
        self.thresholds[pair] = deepcopy(self.thresh_base)