from binance.spot import Spot as BinanceSpot
from pprint import pprint

binance = BinanceSpot()

pprint(binance.depth(symbol="BTCUSDT"))
