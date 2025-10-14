from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# 您的網址
url = "https://flight.eztravel.com.tw/tickets-oneway-tpe-fuk/?outbounddate=11%2F12%2F2025&dport=&aport=&adults=1&children=0&infants=0&direct=true&cabintype=tourist&airline=BR&searchbox=t"

# 假設您已經安裝了 ChromeDriver，且路徑已配置好
# options = webdriver.ChromeOptions()
# options.add_argument('--headless') # 如果不想看到瀏覽器視窗，可開啟無頭模式
# driver = webdriver.Chrome(options=options)
driver = webdriver.Chrome() # 或使用其他瀏覽器，如 Firefox

data = []

try:
    print("正在開啟網頁並等待資料載入...")
    driver.get(url)

    # **重要：等待班機列表出現。您需要找到一個確認資料已載入的元素選擇器**
    # 這裡的選擇器 '.flight-list-item' 是一個假設，您需要檢查網頁原始碼來確認
    flight_items_selector = ".flight-list-item" # 請替換成實際的 CSS 選擇器

    # 等待直到至少一個航班項目出現，最多等 20 秒
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, flight_items_selector))
    )
    print("資料載入完成，開始抓取...")

    # 抓取所有航班列表項目
    flight_items = driver.find_elements(By.CSS_SELECTOR, flight_items_selector)

    for item in flight_items:
        try:
            # **以下選擇器都需要根據您網頁檢查工具中的實際 class/id 來修改！**
            
            # 抓取時間 (假設起飛時間的 class)
            time_element = item.find_element(By.CSS_SELECTOR, ".departure-time") 
            time_str = time_element.text
            
            # 抓取航班 (假設航班編號的 class)
            flight_element = item.find_element(By.CSS_SELECTOR, ".flight-number")
            flight_str = flight_element.text
            
            # 抓取價格 (假設價格的 class)
            price_element = item.find_element(By.CSS_SELECTOR, ".fare-price")
            price_str = price_element.text.replace('TWD', '').strip()

            data.append({
                "時間": time_str,
                "航班": flight_str,
                "價格": price_str
            })
            
        except Exception as e:
            # 忽略抓取特定單一元素失敗的情況，繼續下一個項目
            print(f"抓取單一航班資訊時發生錯誤: {e}")
            continue

except Exception as e:
    print(f"網頁載入或等待時發生錯誤: {e}")
    
finally:
    driver.quit() # 結束瀏覽器會話

# 輸出結果
if data:
    print("\n--- 抓取到的長榮班機資訊 (2025/11/12) ---")
    for item in data:
        print(f"時間: {item['時間']}, 航班: {item['航班']}, 價格: {item['價格']}")
else:
    print("\n未抓取到任何航班資料。請檢查選擇器或網頁結構是否已更改。")