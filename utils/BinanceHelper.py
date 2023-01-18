# =============================================================================
# IMPORTS
# =============================================================================
import requests
from pprint import pprint

# =============================================================================
# CONSTANTS
# =============================================================================
# from utils.constants import BINANCE_GLOBAL_BASEURL
BINANCE_GLOBAL_BASEURL = "https://api.binance.com/api/v3"

# =============================================================================
# HELPER CLASS TO GET TOKEN INFO
# =============================================================================
class BinanceHelper:
    def __init__(self, token):
        self.token = token

    # =============================================================================
    # GET INFO FOR A SPECIFIC TOKEN
    # =============================================================================
    def get_ticksize_from_binance(self):
        res = requests.get(f"{BINANCE_GLOBAL_BASEURL}/exchangeInfo").json()
        info = res["symbols"]
        for symbol in info:
            if symbol["symbol"] == self.token:
                pprint(symbol)
                for filt in symbol["filters"]:
                    if filt["filterType"] == "PRICE_FILTER":
                        tick_size = float(filt["tickSize"])
        return tick_size

    # =============================================================================
    # COMPUTE DIFFERENCE BETWEEN BID AND ASK
    # =============================================================================
    def tick_diff(self, token, bid, ask):
        ticks = round((ask - bid) / self.order_params[token]["price_rounder"], 3)
        return abs(ticks)


if __name__ == "__main__":
    obj = BinanceHelper("BTCBUSD")
    res = obj.get_ticksize_from_binance()
    print(res)
