# 載入 schedule 和 datetime 套件 (這裡用到 datetime 是為了印出本機時間，驗證排程是否正確執行)
import schedule
from datetime import datetime
import time
import keyboard  # 新增: 用於偵測鍵盤輸入


# 把本機時間轉成字串格式，並且印出
def say_hi():
    current_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(current_dt + '\nHi!')

# 載入價值資料爬蟲的程式碼 (這裡為了可讀性，只留下印出「Start working...」字串的程式碼，表示程式碼確實進入了這個函數)
def get_price():
    print('Start working...')

# 先運行 schedule.clear() 將排程清除，避免習慣使用 jupyter notebook 整合開發環境的讀者，
# 有殘存的排程，造成運行結果不如預期
schedule.clear()

# 指定每 15 秒運行一次 say_hi 函數
schedule.every(15).seconds.do(say_hi)

# 每天 15:00 運行一次 get_price 函數
schedule.every().day.at('22:54').do(get_price)

# 將 schedule.run_pending() 放在 while 無窮迴圈內
while True:
    schedule.run_pending()