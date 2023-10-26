#要使用pandas前，需先在命令列中執行安裝pip install pandas
#才能import 
import pandas as pd

#要使用pandas DataFrame雙維度資料
#建立pandas DataFrame物件
grades = {              #用字典建立資料
    "name":["小花","大明","中二"],
    "math":[50,60,70],
    "chinese":[70,80,90]
}
my_dataframe = pd.DataFrame(grades)     #建立物件並將資料放入
my_dataframe.index = [1,2,3] #列的最前面標題
my_dataframe.columns = ["姓名","國文","英文"] #欄的標題
# cl

#使用list建立資料
list_grades = [              #用字典建立資料
    ["小花",50,70],
    ["大明",60,80],
    ["中二",70,90]
]

list_dataframe = pd.DataFrame(list_grades)
print("使用lsit建立物件")
print("****************")
print(list_dataframe)
print()
print("***head(2)取前面兩筆****")
print(list_dataframe.head(2))

print()
print("***tail(2)取後面兩筆****")
print(list_dataframe.tail(2))

print()
print("***取得單一欄位資料(型別為Series)****")
print(grades-1["name"])

print()
print("***取得單一欄位資料(型別為DataFrame)****")
print(grades-1[["name"]])

# # data=pd.Series([20,10,15])
# data=pd.DataFrame({
#     "name":["Amy","John","Bob"],
#     "salary":[30000,50000,60000]
# })

# data=data*2
# print(data)

# print(data["name"])  #取得特定的欄位

# print(data)
# print("*************")
# print(data.iloc[1])