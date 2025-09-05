import requests

symbols = ["2330.TW", "2317.TW"]
url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(symbols)}"

response = requests.get(url)
data = response.json()
for stock in data['quoteResponse']['result']:
    print(f"{stock['symbol']} 即時股價：{stock['regularMarketPrice']}")