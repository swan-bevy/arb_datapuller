# =============================================================================
# IMPORTS
# =============================================================================
from discord import SyncWebhook
from utils.decimal_helper import dec
from utils.jprint import jprint
from utils.time_helpers import determine_cur_utc_timestamp, convert_datetime_str_to_obj

DISCORD_URL = "https://discord.com/api/webhooks/1022260697037541457/sH6v5xoDBSykaEyn1W91GtesMVC6PurG8ksESCbwR5VlxXi9FXFWlrc-OmnHQzA7RBWN"

# =============================================================================
# CONSTANTS
# =============================================================================
LOWER_BOUND_THRESHOLD_MULT = 0.9


class DiscordAlert:
    def __init__(self, diff_pairs):
        self.diff_pairs = diff_pairs
        self.thresholds = self.reset_thresholds()

    # =============================================================================
    # Check $$$ diff between exchanges and alert discord if sufficient.
    # =============================================================================
    def determine_exchange_diff_and_alert_discord(self, bid_asks: dict):
        try:
            msgs = self.determine_exchange_diff(bid_asks)
            self.post_msg_to_discord(msgs)
        except Exception as e:
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
            print("Diff: ", diff)
            cur_thresh = self.determine_cur_threshold(pair)
            surpassed = self.is_diff_bigger_than_cur_thresh(pair, diff, cur_thresh)
            if surpassed:
                msg = self.format_msg_for_discord(pair, diff, cur_thresh)
                msgs.append(msg)
            elif not surpassed:
                self.check_if_below_th_and_deactivate(pair, diff, cur_thresh)
        return msgs

    # =============================================================================
    # Compute difference between prices, correctly formatted and rounded
    # =============================================================================
    def compute_price_diff(self, bid_ask0: dict, bid_ask1: dict):
        mid0, mid1 = bid_ask0["mid"], bid_ask1["mid"]
        diff = abs(dec(mid0) - dec(mid1))
        return float(diff)

    # =============================================================================
    # Determine the current minimum threshold
    # =============================================================================
    def determine_cur_threshold(self, pair: str):
        threshs = self.thresholds[pair]
        if not threshs["low"]["surpassed"]:
            return "low"
        elif not threshs["mid"]["surpassed"]:
            return "mid"
        elif not threshs["high"]["surpassed"]:
            return "high"
        else:
            return "surpassed_all"

    # =============================================================================
    # Determine the current minimum threshold
    # =============================================================================
    def check_if_thresh_active(self, threshs: dict, level: str):
        surpassed = threshs[level]["surpassed"]  # has this thresh been activated
        last_triggered = threshs[level]["timestamp"]
        if last_triggered is None:
            return surpassed
        now = determine_cur_utc_timestamp()
        minutes_apart = ((now - last_triggered).seconds) / 60

        ### CONTINUE HERE!!!
        return surpassed or minutes_apart < 60

    # =============================================================================
    # Check if difference goes above
    # =============================================================================
    def is_diff_bigger_than_cur_thresh(self, pair: str, diff: float, cur_thresh: str):
        if cur_thresh == "surpassed_all":
            return False
        thresh_val = self.thresholds[pair][cur_thresh]["value"]
        if diff > thresh_val:
            self.thresholds[pair][cur_thresh]["surpassed"] = True  ## Important
            return True
        return False

    # =============================================================================
    # Check if we went below the previous threshold, deactivate if so
    # Don't take exact thesh value, take something 10% below
    # =============================================================================
    def check_if_below_th_and_deactivate(self, pair: str, diff: float, cur_thresh: str):
        prev_thresh = self.determine_prev_threshold(cur_thresh)
        if prev_thresh is None:
            return
        prev_val = self.thresholds[pair][prev_thresh]["value"]
        prev_val = prev_val * LOWER_BOUND_THRESHOLD_MULT
        if diff < prev_val:
            self.thresholds[pair][prev_thresh]["surpassed"] = False  ## Important

    # =============================================================================
    # Determine the most recent/previous surpassed threshold
    # =============================================================================
    def determine_prev_threshold(self, cur_thresh: str):
        if cur_thresh == "low":
            return None  # No threshold active
        elif cur_thresh == "mid":
            return "low"
        elif cur_thresh == "high":
            return "mid"
        elif cur_thresh == "surpassed_all":
            return "high"

    # =============================================================================
    # Format message for discord webhook
    # =============================================================================
    def format_msg_for_discord(self, pair: str, diff: float, cur_thresh: str):
        thresh_val = self.thresholds[pair][cur_thresh]["value"]

        msg0 = f"ALERT: Arbitrage opportunity.\n"
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
    # Set/reset thresholds
    # =============================================================================
    def reset_thresholds(self):
        thresholds = {}
        for pair in self.diff_pairs:
            thresh = {
                "low": {"value": 1, "surpassed": False, "timestamp": None},
                "mid": {"value": 5, "surpassed": False, "timestamp": None},
                "high": {"value": 10, "surpassed": False, "timestamp": None},
            }
            thresholds[pair] = thresh
        return thresholds


if __name__ == "__main__":
    disc = DiscordAlert(["DYDX-FTX_US"])
    bid_asks = [
        {"DYDX": {"mid": 99.8}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 110}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 110}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 110}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 110}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 99}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 99}, "FTX_US": {"mid": 99}},
        {"DYDX": {"mid": 99}, "FTX_US": {"mid": 99}},
        # {"DYDX": {"mid": 120}, "FTX_US": {"mid": 99}},
    ]
    for b_a in bid_asks:
        disc.determine_exchange_diff_and_alert_discord(b_a)
