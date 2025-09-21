import requests
import pandas as pd

# 讀取台灣證券交易所的資料
stock_id = "2330"  # 台積電
url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
data = requests.get(url).json()

df = pd.DataFrame(data["data"], columns=data["fields"])
print(df)

# 存入CSV檔案
df.to_csv("D:\Python_repo\python\Stock_day_data\\" + stock_id +"_202509.csv", index=False, encoding="big5")
