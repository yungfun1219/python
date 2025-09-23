import requests
import pandas as pd
import sqlite3
from datetime import datetime

# 讀取台灣證券交易所的資料
stock_id = "2330"  # 台積電
url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
data = requests.get(url).json()

df = pd.DataFrame(data["data"], columns=data["fields"])

# 建立資料庫連接
db_path = r"D:\Python_repo\python\Stock_day_data\stock_SQLite.db"
conn = sqlite3.connect(db_path)
#
try:
    #sqlstr = 'CREATE TABLE 產品(編號 INTEGER PRIMARY KEY, 名稱 TEXT, 價格 REAL)'
    #conn.execute(sqlstr)
    #conn.commit()

    sqlstr = 'INSERT INTO 產品(編號, 名稱, 價格) VALUES (0003, "產品C", 3000)'
    conn.execute(sqlstr)
    sqlstr = 'INSERT INTO 產品(編號, 名稱, 價格) VALUES (0004, "產品D", 4000)'
    conn.execute(sqlstr)
    conn.commit()
    print("資料表建立成功")
except Exception as e:
    print("資料表已存在或建立失敗:", e)
#
#print(f"已連接到資料庫 {db_path}")
# 將資料寫入資料庫
table_name = f"stock_{stock_id}"
df.to_sql(table_name, conn, if_exists='replace', index=False)


# 關閉資料庫連接
conn.close()

print(f"資料已成功存入資料庫 {db_path}")

# 存入CSV檔案 (保留原有功能)
df.to_csv(r"D:\Python_repo\python\Stock_day_data\\" + stock_id + "_202509.csv", index=False, encoding="big5")