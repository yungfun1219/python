# 載入Selenium 相關模組
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

# 設定Chrome Driver
options=Options()
options.chrome_exextable_path="F:\SynologyDrive\github\python\study\chromedriver.exe"

# 建立Driver物件實體，用程式操作瀏覽器運作
driver=webdriver.Chrome(options=options)
driver.get("https://www.linkedin.com/jobs/search?trk=guest_homepage-basic_guest_nav_menu_jobs&position=1&pageNum=0")
# 捲動視窗，等待瀏覽器載入更多資訊
    # 捲動視窗到底部
n=0
while n<3:
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight);") 
    time.sleep(3)
    n+=1
    # 取得網頁中工作的標籤
titleTags=driver.find_elements(By.CLASS_NAME,"base-search-card__title")
for titleTag in titleTags:
    print(titleTag.text)
driver.close()
