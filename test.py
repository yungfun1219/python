import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

# 爬上 header
begin = '2021/01/01'
stocks = '2330'

headers = {
    'content-type': 'text/html; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'
}

# 目標網址
baseurl = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={}&stockNo={}".format(begin, stocks)

# 加上 content 解決某些回傳失敗的問題
data = requests.get(url=baseurl, headers=headers).content
title = BeautifulSoup(data, 'html.parser').find('thead').find('tr')

# 上一節爬表格的做法
datalist = []
for col in title.find_all_next('tr'):
    datalist.append([row.text for row in col.find_all('td')])

# 把第一行不需要的數據拿掉
for each in datalist[1:]:
    each[0] = transform_date(each[0])

# 轉成 pandas 的 DataFrame
#df = pd.DataFrame(datalist[1:], columns=datalist[0])
#df.columns = datalist[0]

print(datalist)
