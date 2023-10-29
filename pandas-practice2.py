#要使用pandas前，需先在命令列中執行安裝pip install pandas，才能import 
# 載入pandas模組
import pandas as pd

""" 篩選練習
# data = pd.Series([5,4,-2,3,7])
# condition=data>3
# print(condition)
# filteredData=data[condition]
# print(filteredData)
# 字串篩選

data = pd.Series(["您好","Python","Pandas"])
# condition=[False,True,False]
condition=data.str.contains("P")
print(condition)
filteredData=data[condition]
print(filteredData)

"""
# data = pd.DataFrame({
#     "name":["Amy","Bob","Charles"],
#     "salary":[30000, 40000, 50000]
# }, index=["a","b","c"])
# print(data)
# ------------

# 觀察資料
# print("資料型態:", data.size)
# print("資料形狀(列, 欄):", data.shape)
# print("資料索引:", data.index)
# --------------

# 取得列(row)的series資料: 根據順序、根據索引
# print("取得第二列",data.iloc[1],sep="\n")
# print("**************")
# print("取得第c列",data.loc["c"],sep="\n")
# --------------

# 取得欄
# print("**************")
# print("取得name欄位", data["name"], sep="\n")

# names = data["name"]    #取得單維度的資料
# print("把name全部轉大寫",names.str.upper(),sep="\n")

# 取得薪水的平均值
# salaries=data["salary"]
# print("取得薪水平均值:", salaries.mean())

# 建立新的欄位
# data["revenue"]=[500000,400000,300000]  #新欄位
# data["rank"]=pd.Series([3,6,1], index=["a","b","c"]) #新欄位
# data["cp"]=data["revenue"]/data["salary"]
# print(data)
