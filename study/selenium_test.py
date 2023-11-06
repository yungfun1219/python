# 載入Selenium 相關模組
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# 設定Chrome Driver
options=Options()
options.chrome_exextable_path="F:\SynologyDrive\github\python\study\chromedriver.exe"

# 建立Driver物件實體，用程式操作瀏覽器運作
driver=webdriver.Chrome(options=options)
driver.get("https://www.ptt.cc/bbs/Stock/index.html")
tags=driver.find_elements(By.CLASS_NAME,"title") #搜尋class 屬性是title的標題
for tag in tags:
    print(tag.text)
    # 取得上一頁的文章標題
link=driver.find_element(By.LINK_TEXT,"‹ 上頁")
link.click()

tags=driver.find_elements(By.CLASS_NAME,"title") #搜尋class 屬性是title的標題
for tag in tags:
    print(tag.text)
    
driver.close()
# 標籤 WebElement
# 超連結標籤<a>
# By.LINK_TEXT

# 操作標籤
# 取得標籤內部的文字
# 取 PTT Stock 版