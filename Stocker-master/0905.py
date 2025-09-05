import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

stock_id = '2330'

url = f"https://tw.quote.finance.yahoo.net/quote/q?type=ta&perd=d&mkt=10&sym={stock_id}&v=1&callback=jQuery111302872649618000682_1649814120914&_=1649814120915"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
res = requests.get(url,headers=headers)

# 最新價格
current = [l for l in res.text.split('{') if len(l)>=60][-1]
current = current.replace('"','').split(',')
# 昨日價格
yday = float(re.search(':.*',[l for l in res.text.split('{') if len(l)>=60][-2].split(',')[4]).group()[1:])

df = pd.DataFrame({
    'open':float(re.search(':.*',current[1]).group()[1:]),
    'high':float(re.search(':.*',current[2]).group()[1:]),
    'low':float(re.search(':.*',current[3]).group()[1:]),
    'close':float(re.search(':.*',current[4]).group()[1:]),
    'volume':float(re.search(':.*',current[5].replace('}]','')).group()[1:]),
    'pct':round((float(re.search(':.*',current[4]).group()[1:])/yday-1)*100,2)
},index=[stock_id])

df