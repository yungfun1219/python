# header偽裝瀏覽器
import requests
# 以高鐵訂票為例https://irs.thsrc.com.tw/IMINT
url = "https://irs.thsrc.com.tw/IMINT"
userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
responseHtml = requests.get(url,headers={
    "User-Agent":userAgent
})
if responseHtml.status_code == requests.codes.ok:   #判斷值200
    responseHtml.encoding = "utf-8"                 #解碼中文
    # print(responseHtml.text)
    
# 使用beautifulsoup網頁解析模組
# pip install -U beautifulsoup4  (測試過不用-U也可以)

from bs4 import BeautifulSoup
bsObject = BeautifulSoup(responseHtml.text,"html.parser")
print(bsObject.h1.text)
print("**********")
# for n in bsObject.title.text:
    # print(bsObject.title.text)