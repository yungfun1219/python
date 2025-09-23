import pandas as pd
import sqlite3
from datetime import datetime


# 建立資料庫連接
db_path = r"D:\Python_repo\python\Stock_day_data\stock_SQLite.db"
conn = sqlite3.connect(db_path)
sqlstr = "SELECT * FROM 產品"
cursor = conn.execute(sqlstr)
rows = cursor.fetchall()
for row in rows:
    print(row)


# 關閉資料庫連接
cursor.close()
conn.close()

