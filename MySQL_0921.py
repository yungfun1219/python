#讀取資料庫
import pandas as pd
import requests #讀取網頁資料
import json  #處理json格式資料  
import csv  #處理csv格式資料
import os   #處理檔案路徑   
import sqlite3  #處理sqlite資料庫
import matplotlib.pyplot as plt  #繪圖  
import numpy as np  #數學運算
import pymysql
conn = pymysql.connect(host='localhost',
                       user='root',
                       password='password',
                       database='test_db',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()
# 讀取台灣證券交易所的資料
cursor.execute("SELECT * FROM stock_data WHERE stock_id = '2330'")
result = cursor.fetchall()
print(result)

for row in result:
    print(row)
    
cursor.close()
conn.close()
