# =============================================================================
# IMPORTS
# =============================================================================
from discord import SyncWebhook
from decimal_helper import dec

DISCORD_URL = "https://discord.com/api/webhooks/1022260697037541457/sH6v5xoDBSykaEyn1W91GtesMVC6PurG8ksESCbwR5VlxXi9FXFWlrc-OmnHQzA7RBWN"


class DiscordAlert:
    def __init__(self):
        self.thresholds = {"low": {}, "mid": {}, "high": {}}

    # =============================================================================
    # Check $$$ diff between exchanges and alert discord if sufficient.
    # =============================================================================
    def determine_exchange_diff_and_alert_discord(self, bid_asks: list):
        try:
            msgs = self.determine_exchange_diff(bid_asks)
            self.post_msg_to_discord(msgs)
        except Exception as e:
            print(f"Discrod webhook failed with this message: {e}")

    # =============================================================================
    # Check $$$ diff between exchanges
    # =============================================================================
    def determine_exchange_diff(self, bid_asks: list):
        msgs = []
        for i, cur in enumerate(bid_asks[:-1]):
            _next = bid_asks[i + 1]
            exchange0, exchange1 = cur["exchange"], _next["exchange"]
            mid_diff = self.compute_price_diff(cur["mid"], _next["mid"])

            cur_thresh = self.determine_cur_threshold()

            if self.is_diff_bigger_than_cur_thresh(mid_diff):
                msg = self.format_msg_for_discord("Mid", exchange0, exchange1, mid_diff)
                msgs.append(msg)
        return msgs

    # =============================================================================
    # Compute difference between prices, correctly formatted and rounded
    # =============================================================================
    def compute_price_diff(self, price0, price1):
        diff = abs(dec(str(price0)) - dec(str(price1)))
        return float(diff)

    # =============================================================================
    # Check if difference goes above
    # =============================================================================
    def is_diff_bigger_than_cur_thresh(self, diff):
        if cur_thresh is None:
            return
        if diff > cur_thresh["val"]:
            idx = cur_thresh["idx"]
            self.thresholds[idx] = True
            return True

    # =============================================================================
    # Determine the current minimum threshold
    # =============================================================================
    def determine_cur_threshold(self):
        for idx, thresh in enumerate(self.thresholds):
            if thresh[1] is False:
                return {"val": thresh[0], "idx": idx}

    # =============================================================================
    # Format message for discord webhook
    # =============================================================================
    def format_msg_for_discord(
        self, _type: str, exchange0: str, exchange1: str, diff: float
    ):
        return f"ALERT: Arbitrage opportunity.\n {_type} price difference between {exchange0} and {exchange1} is: ${diff}"

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
