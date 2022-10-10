from binance.spot import Spot
from pprint import pprint

binance = Spot()

pprint(binance.depth("BTCUSDT"))
