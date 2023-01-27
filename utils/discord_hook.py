# =============================================================================
# IMPORTS
# =============================================================================
import requests, traceback, json, sys
from discord import SyncWebhook

# =============================================================================
# Post messages to discord
# =============================================================================
def post_msgs_to_discord(url, msgs):
    try:
        webhook = SyncWebhook.from_url(url)
        if type(msgs) is str:
            webhook.send(msgs)
        elif type(msgs) is list:
            for msg in msgs:
                webhook.send(msg)
    except:
        print("Failed pinging dicord")


# =============================================================================
# DISCORD ALERT
# =============================================================================
from utils.constants import DISCORD_PERSONAL

DSC_HEADERS = {"Content-Type": "application/json"}
DSC_SEPARATOR = "======================================================"


def ping_private_discord(msg):
    try:
        payload = handle_type_of_msg(msg)
        payload = {"content": payload}
        res = requests.post(
            DISCORD_PERSONAL, data=json.dumps(payload), headers=DSC_HEADERS
        )
        return res
    except:
        traceback.print_exc()


def handle_type_of_msg(msg):
    if isinstance(msg, BaseException):
        _type, _value, _traceback = sys.exc_info()
        formated = "".join(traceback.format_exception(_type, _value, _traceback))
    else:
        formated = str(msg)
    return f"{DSC_SEPARATOR}\n{formated}\n{DSC_SEPARATOR}"
