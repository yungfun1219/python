#要使用pandas前，需先在命令列中執行安裝pip install pandas，才能import 
# 載入pandas模組
import pandas as pd

# 資料索引
# data = pd.Series([5,4,-2,3,7],index=["a","b","c","d","e"])
# print(data)
# 觀察資料
# print("資料型態:", data.dtype)
# print("資料數量:", data.size)
# print("資料索引:", data.index)
# --------------

# 收取資料:根據順序、根據索引
# print(data[2], data[0])
# print(data["e"], data["d"])
# ----------------

# 數字運算:基本、統計、順序
# data1 = pd.Series([5,4,-2,3,7],index=["a","b","c","d","e"])
# print("最大值:",data1.max())
# print("總和:",data1.sum())
# print("標準差:",data1.std())
# print("中位數:",data1.median())
# print("最大的三個數:",data1.nlargest(3))
# -------------

data2=pd.Series(["你好","Python","Pandas"])
# 字串運算: 基本、串接、搜尋、取代
# print(data2.str.lower())
# print(data2.str.len())
# print(data2.str.cat(sep="-"))   #串接的符號
# print(type(data2.str.contains("P")))
print(data2.str.replace("你好","Hello"))  
print(data2)

#要使用pandas DataFrame雙維度資料
#建立pandas DataFrame物件
# grades = {              #用字典建立資料
#     "name":["小花","大明","中二"],
#     "math":[50,60,70],
#     "chinese":[70,80,90]
# }
# my_dataframe = pd.DataFrame(grades)     #建立物件並將資料放入
# my_dataframe.index = [1,2,3] #列的最前面標題
# my_dataframe.columns = ["姓名","國文","英文"] #欄的標題
# # cl

# #使用list建立資料
# list_grades = [              #用字典建立資料
#     ["小花",50,70],
#     ["大明",60,80],
#     ["中二",70,90]
# ]

# list_dataframe = pd.DataFrame(list_grades)
# print("使用lsit建立物件")
# print("****************")
# print(list_dataframe)
# print()
# print("***head(2)取前面兩筆****")
# print(list_dataframe.head(2))

# print()
# print("***tail(2)取後面兩筆****")
# print(list_dataframe.tail(2))

# print()
# print("***取得單一欄位資料(型別為Series)****")
# print(grades-1["name"])

# print()
# print("***取得單一欄位資料(型別為DataFrame)****")
# print(grades-1[["name"]])

# # # data=pd.Series([20,10,15])
# # data=pd.DataFrame({
# #     "name":["Amy","John","Bob"],
# #     "salary":[30000,50000,60000]
# # })

# # data=data*2
# # print(data)

# # print(data["name"])  #取得特定的欄位

# # print(data)
# # print("*************")
# # print(data.iloc[1])