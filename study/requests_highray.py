# 讀取網站資料的模組requests
# 需安裝pip install -U requests
import requests

url = "https://www.thsrc.com.tw/"

# http Headers偽裝瀏覽器
headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                }
# 當需要偽裝時
getResp = requests.get(url,headers=headers)
# 解碼中文
getResp.encoding="UTF-8"

# 印出HTML編碼內容
print(getResp.text)
print("***以上get***")

# 狀態碼200 : requests.code.ok
print("getResp狀態碼:", getResp.status_code)
print("*******")

