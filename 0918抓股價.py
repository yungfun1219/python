# import yfinance as yf

# 台積電 2330
#stock = yf.Ticker("2330.TW")

# 抓取近一個月的股價
#df = stock.history(period="1mo")
#print(df)
#---
import requests
import json

url = "https://tw.quote.finance.yahoo.net/quote/q"
params = {
    "type": "ta",
    "perd": "1m",
    "mkt": "10",
    "sym": "2330"   # 台積電
}

res = requests.get(url, params=params)
data = json.loads(res.text.split('=')[1])  # 回傳格式是 "callback({...})"
print(data)

# 五檔委買、委賣
bids = data["ta"][0]["b"]  # 委買價
bid_vols = data["ta"][0]["bv"]  # 委買量
asks = data["ta"][0]["a"]  # 委賣價
ask_vols = data["ta"][0]["av"]  # 委賣量

print("委買:", list(zip(bids, bid_vols)))
print("委賣:", list(zip(asks, ask_vols)))
