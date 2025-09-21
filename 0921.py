import requests
import pandas as pd

stock_id = "2330"  # 台積電
url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
data = requests.get(url).json()

df = pd.DataFrame(data["data"], columns=data["fields"])
#print(df)
#-----

df = pd.read_csv("D:\Python_repo\python\STOCK_DAY_2330_202509.csv", encoding="utf-8")
print(df) 
#file = open("STOCK_DAY_2330_202509", "w+", encoding="utf-8")
#print(file.name)
#print(file.mode)
#print(file.closed)
file1 = [line for line in open("D:\Python_repo\python\STOCK_DAY_2330_202509.csv", "r", encoding="utf-8")]
print(file1[1:10])  # 顯示前10筆資料
#file.close()
print("*****")
#print(file.closed)
file2 = [line2.strip("\n") for line2 in file1]
print(file2[1:])  # 顯示前10筆資料
