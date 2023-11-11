# 讀取網站資料的模組requests
# 需安裝pip install -U requests
import requests

geturl = "http://httpbin.org/get"
posturl = "http://httpbin.org/post"

# http Headers偽裝瀏覽器
headersAgent = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                }
# 當需要偽裝時
getResp = requests.get(geturl,headers=headersAgent)
# url可以提供參數
payload = {"key1":"value1","key2":"value2"}
# 可得到http://www.kt-perfect.com.tw/get?key1=value1&key2=value2
getResp = requests.get(geturl,params=payload)

postResp = requests.post(posturl,data=payload)

# 設定物件
# resp = requests.get(url)

# 解碼中文
getResp.encoding="UTF-8"
postResp.encoding="UTF-8"

# 印出HTML編碼內容
print(getResp.text)
print("***以上get***")

print(postResp.text)
print("***以上post****")

# 狀態碼200 : requests.code.ok
print("getResp狀態碼:", getResp.status_code)
print("postResp狀態碼:", postResp.status_code)
print("*******")

