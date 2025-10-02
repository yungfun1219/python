# 載入 schedule 和 datetime 套件
import schedule
from datetime import datetime
import time
import keyboard  # 新增: 用於偵測鍵盤輸入

# --- 函數定義 ---

def say_hi():
    """將本機時間轉成字串格式，並且印出。"""
    current_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{current_dt} Hi!")

def get_price():
    """載入價值資料爬蟲的程式碼模擬。"""
    print('Start working...')

# --- 排程設定與執行 ---

def run_scheduler_with_exit():
    """設定排程並運行，直到偵測到 ESC 鍵。"""
    
    # 1. 清除舊排程
    schedule.clear()
    
    # 2. 設定排程任務
    # 指定每 15 秒運行一次 say_hi 函數
    schedule.every(15).seconds.do(say_hi)
    
    # 每天 15:00 運行一次 get_price 函數 (注意：此任務需等待指定時間執行)
    schedule.every().day.at('23:41').do(get_price)
    
    print("\n--- 排程程式啟動 ---")
    print("排程任務運行中...")
    print("隨時按下 [ESC] 鍵可終止程式。")
    print("---------------------\n")
    
    # 3. 運行排程並監聽按鍵
    while True:
        # 運行所有等待執行的排程任務
        schedule.run_pending()
        
        # 監聽是否按下 ESC 鍵
        if keyboard.is_pressed('esc'):
            print("\n-------------------------")
            print("偵測到 [ESC] 鍵，程式即將終止...")
            print("-------------------------")
            break  # 跳出 while 迴圈
            
        # 避免 CPU 佔用過高，讓程式休眠短暫時間
        time.sleep(1)

# 執行排程主函數
if __name__ == "__main__":
    run_scheduler_with_exit()