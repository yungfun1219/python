import pandas as pd
# 讀取資料
data = pd.read_csv("googleplaystore.csv")    #讀取資料成DataFrame

# 觀察資料
"""
# print(data)
print("資料數量",data.shape)
print("資料欄位",data.columns)
print("*************")
print(data["Rating"])
# 分析資料:評分的各種統計數據
condition = data["Rating"]<=5
data=data[condition]
print(data["Rating"].nlargest(1000))
print("平均數",data["Rating"].mean())
print("中位數",data["Rating"].median())
print("去的前100個應用程式的平均",data["Rating"].nlargest(1000).mean())
"""
# 分析資料:安裝數量及各種統計數據
# data["Installs"]=pd.to_numeric(data["Installs"].str.replace("[,+]",""))
# print(data["Installs"])
# print(data["Installs"][10472])
# print(data["Installs"].mean())

# 基於資料的應用:關鍵字搜尋應用程式名稱
keyword=input("請輸入關鍵字:")
condition=data["App"].str.contains(keyword, case=False)
print("包含關鍵字的應用程式數量:",data[condition].shape[0])