# =============================================================================
# IMPORTS
# =============================================================================
import traceback
from utils.jprint import jprint
from utils.discord_hook import post_msgs_to_discord
from utils.constants import DISCORD_URL
import datetime as dt

# =============================================================================
# This class check bid_ask data for exchange arbitrage opportunities and send to Discord
# =============================================================================
class FrozenOrderbook:
    def __init__(self, Caller):
        self.Caller = Caller
        self.alert_limits = self.create_alert_limit()
        self.window = self.ask_user_for_frozen_orderbook_window()

    # =============================================================================
    # Check if bid/asks stayed the same for n last fetches
    # =============================================================================
    def check_all_orderbooks_if_frozen(self):
        try:
            self.check_orderbooks_if_frozen()
        except Exception as e:
            traceback.print_exc()

    # =============================================================================
    # Actual function
    # =============================================================================
    def check_orderbooks_if_frozen(self):
        for ex, df in self.Caller.df_obj.items():
            if len(df.index) <= self.window:
                continue
            rows = df.tail(self.window)
            self.check_specific_orderbook(ex, rows)

    # =============================================================================
    # Check if a specific orderbook is frozen
    # Check if all vals in col are not the same and exit,
    # =============================================================================
    def check_specific_orderbook(self, ex, rows):
        print(rows)
        bid_p = rows["bid_price"]
        bid_s = rows["bid_size"]
        ask_p = rows["ask_price"]
        ask_s = rows["ask_size"]
        if bid_p.max() != bid_p.min():
            return
        if bid_s.max() != bid_s.min():
            return
        if ask_p.max() != ask_p.min():
            return
        if ask_s.max() != ask_s.min():
            return
        self.alert_discord_of_frozen_orderbook(ex)

    # =============================================================================
    # Alert discord that the orderbook is stuck.
    # =============================================================================
    def alert_discord_of_frozen_orderbook(self, ex):
        if self.check_if_active_alert_limit(ex):
            return

        msg0 = f"ALERT: FROZEN ORDERBOOK\n"
        msg1 = f"{ex} trading {self.Caller.market} at interval {self.Caller.interval} seconds.\n"
        msg2 = (
            f"The orderbook has stayed the same for the last {self.window} requests.\n"
        )
        msg = msg0 + msg1 + msg2
        post_msgs_to_discord(DISCORD_URL, msg)

    # =============================================================================
    # Ask user how many rows back we should check for identical orderbooks
    # =============================================================================
    def check_if_active_alert_limit(self, exchange):
        print("Orderbook frozen...")
        cur_limit = self.alert_limits[exchange]
        now = dt.datetime.utcnow()
        diff = now - cur_limit
        hours = diff.total_seconds() / (60 * 60)
        print(cur_limit, now, hours)
        if hours < 1:
            jprint("Limit in place:", self.alert_limits)
            return True
        self.alert_limits[exchange] = now

    # =============================================================================
    # Ask user how many rows back we should check for identical orderbooks
    # =============================================================================
    def ask_user_for_frozen_orderbook_window(self):
        return int(input("Enter frozen orderbook window: "))

    # =============================================================================
    # Create an alert limit on the max amount so we don't spray 'n pray alerts
    # =============================================================================
    def create_alert_limit(self):
        limit = {}
        for ex in self.Caller.exchanges:
            limit[ex] = dt.datetime.utcnow() - dt.timedelta(hours=1)
        return limit
