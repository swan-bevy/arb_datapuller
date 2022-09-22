# =============================================================================
# IMPORTS
# =============================================================================
from discord import SyncWebhook
from decimal import Decimal

DISCORD_URL = "https://discord.com/api/webhooks/1022260697037541457/sH6v5xoDBSykaEyn1W91GtesMVC6PurG8ksESCbwR5VlxXi9FXFWlrc-OmnHQzA7RBWN"
# =============================================================================
# Check $$$ diff between exchanges and alert discord if sufficient.
# =============================================================================
def determine_exchange_diff_and_alert_discort(bid_asks: list):
    msgs = []

    for i, cur in enumerate(bid_asks[:-1]):
        _next = bid_asks[i + 1]
        exchange0, exchange1 = cur["exchange"], _next["exchange"]
        bid_diff = compute_price_diff(cur["ask_price"], _next["ask_price"])
        ask_diff = compute_price_diff(cur["bid_price"], _next["bid_price"])
        mid_diff = compute_price_diff(cur["mid"], _next["mid"])

        if ask_diff > 5:
            msg = format_msg_for_discord("Ask", exchange0, exchange1, ask_diff)
            msgs.append(msg)
        if bid_diff > 5:
            msg = format_msg_for_discord("Bid", exchange0, exchange1, bid_diff)
            msgs.append(msg)
        if mid_diff > 5:
            msg = format_msg_for_discord("Mid", exchange0, exchange1, mid_diff)
            msgs.append(msg)
    for msg in msgs:
        post_msg_to_discord(msg)


# =============================================================================
# Compute difference between prices, correctly formatted and rounded
# =============================================================================
def compute_price_diff(price0, price1):
    diff = abs(dec(str(price0)) - dec(str(price1)))
    return float(diff)


# =============================================================================
# Convert float to decimal
# =============================================================================
def dec(num):
    return Decimal(str(num))


# =============================================================================
# Format message for discord webhook
# =============================================================================
def format_msg_for_discord(_type: str, exchange0: str, exchange1: str, diff: float):
    return f"ALERT: Arbitrage opportunity. {_type} price difference between {exchange0} and {exchange1} is: ${diff}"


# =============================================================================
# Post messages to discord
# =============================================================================
def post_msg_to_discord(msgs):
    webhook = SyncWebhook.from_url(DISCORD_URL)
    if type(msgs) is str:
        webhook.send(msgs)
    elif type(msgs) is list:
        for msg in msgs:
            webhook.send(msg)
