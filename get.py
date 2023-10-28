# 先在終端機安裝模組
# pip install -U requsets
# 若需要升級則 python.exe -m pip install --upgrade pip

# 程式內使用
# 讀取網頁
# get 請求
import requests
# url = "http://www.kt-perfect.com.tw"
# responseHtml = requests.get(url)
# if responseHtml.status_code == requests.codes.ok:   #判斷值200
#     responseHtml.encoding = "utf-8"                 #解碼中文
    # print(responseHtml.text)
    
# 測試網站
# httpbin.org/get
# httpbin.org/post

# url1 = "http://httpbin.org/get"
# payload = { 
#            "key1":"value1" , 
#            "key2":"value2" }      #用字典型態
# responseHtml1 = requests.get(url1, params=payload)
# print(responseHtml1.text)

# post 請求 (在html中表單的傳遞)
url2 = "http://httpbin.org/post"
payload2 = { 
           "key1":"value1" , 
           "key2":"value2" }      #用字典型態
responseHtml2 = requests.post(url2, params=payload2)
print(responseHtml2.text)
