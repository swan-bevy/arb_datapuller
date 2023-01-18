# =============================================================================
# IMPORTS
# =============================================================================
import requests

# =============================================================================
# Class to check orderbook diff between bid and ask
# =============================================================================
class DydxHelper:
    # =============================================================================
    # Get tick size from dydx to check for lose orderbook
    # =============================================================================
    def get_ticksize_from_dydx(self):
        res = requests.get("https://api.dydx.exchange/v3/markets")
        res = res.json()["markets"]["XYZ"]
        return float(res["tickSize"])


if __name__ == "__main__":
    obj = DydxHelper()
    print(obj.get_ticksize_from_dydx())
