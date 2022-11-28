import requests
from pprint import pprint

base_url = "https://api.binance.com/api/v3/exchangeInfo?symbol="
symbol = "BTCBUSD"
res = requests.get(base_url + symbol, headers={"accept": "application/json"}).json()
pprint(res)
