# =============================================================================
# IMPORTS
# =============================================================================
import traceback
from decimal import Decimal
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
    def __init__(self, Caller):
        self.Caller = Caller

        self.thresh_base = self.generate_thresh_base_dict()
        self.thresholds = {
            p: deepcopy(self.thresh_base) for p in self.Caller.diff_pairs
        }
        self.thresh_incr = self.ask_for_thresh_incrementer()
        self.max_bid_ask_spread = 0.15
        self.thresh_reset_time = SECS_PER_HOUR

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
        jprint("Threshs: ", self.thresholds)
        for pair in self.Caller.diff_pairs:
            if self.check_if_orderbook_is_loose(bid_asks, pair):
                continue
            mids = self.extract_mid_prices(bid_asks, pair)
            diff = self.compute_price_diff(mids)
            cur_thresh = self.check_thresh_and_reset_if_necessary(pair)

            if cur_thresh["value"] < diff["pct"]:
                msg = self.format_msg_for_discord(pair, mids, diff)
                msgs.append(msg)
                self.increase_and_update_threshold(pair, diff)
        return msgs

    # =============================================================================
    # Check if the bid-ask spread is tight
    # =============================================================================
    def check_if_orderbook_is_loose(self, bid_asks: dict, pair: str):
        for ex in pair.split("-"):
            bid, ask = bid_asks[ex]["bid_price"], bid_asks[ex]["ask_price"]
            diff = abs(bid - ask) / bid * 100
            if diff > self.max_bid_ask_spread:
                print("Loose orderbook. ")
                return True  # orderbook loose
        return False  # orderbook is tight

    # =============================================================================
    # Extract mid prices from bid_ask dict
    # =============================================================================
    def extract_mid_prices(self, bid_asks: dict, pair: str) -> dict:
        ex0, ex1 = pair.split("-")
        mid0 = dec(bid_asks[ex0]["mid"])
        mid1 = dec(bid_asks[ex1]["mid"])
        return {ex0: mid0, ex1: mid1}

    # =============================================================================
    # Compute difference between prices, correctly formatted and rounded
    # =============================================================================
    def compute_price_diff(self, mids: dict):
        mid0, mid1 = list(mids.values())
        abs_diff = abs(mid0 - mid1)
        avg = (mid0 + mid1) / 2
        pct_diff = abs_diff / avg * 100
        return {"abs": float(round(abs_diff, 3)), "pct": float(round(pct_diff, 3))}

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
    def increase_and_update_threshold(self, pair, diff):
        new_val = dec(diff["pct"]) + dec(self.thresh_incr)
        new_timestamp = determine_cur_utc_timestamp()
        self.thresholds[pair] = {"value": new_val, "timestamp": new_timestamp}

    # =============================================================================
    # Format message for discord webhook
    # =============================================================================
    def format_msg_for_discord(self, pair: str, mids: dict, diff: float):
        ex0, ex1 = pair.split("-")
        thresh_val = round(self.thresholds[pair]["value"], 2)
        msg0 = f"ALERT: Arbitrage opportunity.\n"
        msg1 = f"{ex0} & {ex1} trading {self.Caller.market} at interval {self.Caller.interval} seconds:\n"
        msg2 = f"Diff surpassed % threshold of: {thresh_val}%\n"
        msg3 = f"Percentage price difference: {diff['pct']}%\n"
        msg4 = f"Absolute price difference: ${diff['abs']}\n"
        msg5 = f"{ex0}-price: {mids[ex0]}, {ex1}-price: {mids[ex1]}\n"
        return msg0 + msg1 + msg2 + msg3 + msg4 + msg5

    # =============================================================================
    # Reset thresholds
    # =============================================================================
    def reset_thresold(self, pair):
        self.thresholds[pair] = deepcopy(self.thresh_base)

    # =============================================================================
    # Ask user for threshold base value in percent
    # =============================================================================
    def generate_thresh_base_dict(self):
        val = input("Enter the Discord alert (%) threshold base value: ")
        return {"value": float(val), "timestamp": None}

    # =============================================================================
    # Ask user for threshold incrementer
    # =============================================================================
    def ask_for_thresh_incrementer(self):
        val = input("Enter the Discord alert (%) threshold INCREMENTER: ")
        return float(val)
