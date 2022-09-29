# =============================================================================
# IMPORTS
# =============================================================================

from discord import SyncWebhook

# =============================================================================
# Post messages to discord
# =============================================================================
def post_msgs_to_discord(url, msgs):
    webhook = SyncWebhook.from_url(url)
    if type(msgs) is str:
        webhook.send(msgs)
    elif type(msgs) is list:
        for msg in msgs:
            webhook.send(msg)
