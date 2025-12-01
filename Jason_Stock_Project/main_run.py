import os
import pathlib
import time as time_module
from datetime import date, datetime, timedelta, time as time_TimeClass
# 導入自行製做的模組

import date_utils
import data_fetcher
import report_generator

# 導入排程與鍵盤監聽模組
import keyboard
import schedule

# 導入環境變數載入 (如果使用 .env 檔案)
from dotenv import load_dotenv

# --- 全域變數定義 ---
# 設定運行狀態，用於安全退出主迴圈
running = True

def stop_program():
    """設定 running 旗標為 False，用於安全退出主迴圈。"""
    global running
    running = False
    print("\n[退出信號]: 收到 'Q' 指令，準備安全退出程式...")


def main_run():
    """主業務邏輯執行函式，排程或熱鍵觸發。"""
    global running 
    
    if not running:
        print("\n[定時任務]: 偵測到退出信號，跳過本次執行。")
        return

    # 基礎路徑定義
    BASE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    
    # 交易日曆檔案路徑 (自動根據年份生成)
    TRADING_DAY_FILE = BASE_DIR / "datas" / "processed" / "get_holidays" / f"trading_day_2021-{datetime.now().strftime('%Y')}.csv"
    
    print(TRADING_DAY_FILE)
    # 1. --- 交易日檢查與日期設定 (使用 date_utils 模組) ---
    try:
        report_dates = date_utils.get_trading_day_info(TRADING_DAY_FILE)
        daily_date = report_dates["daily_date"]
        monthly_date = report_dates["monthly_date"]
        # start_time = report_dates["start_time"] # 雖有獲取，但在本處未使用
    except Exception as e:
        print(f"❌ 日期時間處理失敗：{e}")
        return # 如果日期處理失敗，則中止後續流程
    
    time_module.sleep(2) # 延遲 2 秒，確保前一步驟 log 輸出完成
    
    # 2. --- 獲取股票清單 ---
    # ❗❗❗ 請替換為實際的股票清單獲取函式 (e.g., data_fetcher.get_stock_list(...) 或從 Excel 獲取)
    #stock_list = ["2330", "2454"] # [待替換] 範例清單 
    # 從 stocks_all.csv 讀取股票清單，並依據【市場別】欄位篩選出「上市」公司。
    STOCKS_ALL_CSV = os.path.join(BASE_DIR, "datas", "raw", "stocks_all.csv")
    stock_list = data_fetcher.get_stock_list(STOCKS_ALL_CSV)
    
    # 3. --- TWSE 報告資料抓取 (使用 data_fetcher 模組) ---
    try:
        data_fetcher.fetch_all_twse_reports(daily_date, monthly_date, stock_list)
    except Exception as e:
        print(f"❌ TWSE 資料抓取失敗：{e}")
        # 即使抓取失敗，也可以繼續執行報告生成，因為可能使用舊資料
        
    # 4. --- 股票分析與報告發送 (使用 report_generator 模組) ---
    # 參數設定
    SHEET_NAME = "股票庫存統計"
    FILTER_COLUMN = "目前股數庫存統計"
    FILTER_VALUE = "0"
    EXCEL_PATH = BASE_DIR / "datas" / "股票分析.xlsx"
    
    # 假設 LINE_USER_ID 已在啟動時載入 (需確保環境變數已設置)
    LINE_USER_ID = os.getenv("LINE_USER_ID") 

    if not LINE_USER_ID:
        print("❌ 警告：未找到 LINE_USER_ID 環境變數，訊息發送將跳過。")
        return

    try:
        report_generator.process_and_send_stock_report(
            excel_path=EXCEL_PATH,
            sheet_name=SHEET_NAME,
            filter_column=FILTER_COLUMN,
            filter_value=FILTER_VALUE,
            trading_day_file_path=TRADING_DAY_FILE,
            base_dir=BASE_DIR,
            date_to_check=report_dates["target_date_slash"],
            now_day_time=report_dates["now_day_time"],
            line_user_id=LINE_USER_ID
        )
    except Exception as e:
        print(f"❌ 報告生成與發送失敗：{e}")


# --- 程式初始化與啟動區塊 ---

# 1. 載入環境變數 (如果使用 .env 檔案)
BASE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
LINE_API_ENV_PATH = BASE_DIR / "line_API.env"
try:
    load_dotenv(LINE_API_ENV_PATH)
    print(f"✅ 環境變數已從 {LINE_API_ENV_PATH} 載入。")
except Exception as e:
    print(f"⚠️ 載入環境變數檔案失敗: {e}。將使用系統預設環境變數。")

# 2. 設定排程
schedule.clear()
schedule.every().day.at('21:00').do(main_run)
print("✅ 已設定定時任務：每日 21:00 執行 main_run。")

# 3. 設定熱鍵
keyboard.add_hotkey('1', main_run)
keyboard.add_hotkey('q', stop_program)
print("✅ 已設定鍵盤熱鍵：[1] 直接執行 main_run, [Q] 停止程式。")

print("\n--- 程式開始運行 ---")
print("主程式和排程監聽中...")

# --- 主循環 (Main Loop) ---
try:
    while running:
        # 1. 檢查是否有排程任務需要運行
        schedule.run_pending()
        
        # 2. 讓主循環短暫休眠 (確保對排程和鍵盤輸入的響應更即時)
        time_module.sleep(1)
        
except KeyboardInterrupt:
    # 允許使用 Ctrl+C 退出
    print("\n程式被 Ctrl+C 中斷退出。")

finally:
    # 3. 移除所有註冊的熱鍵 (清理環境)
    keyboard.unhook_all()
    print("所有鍵盤監聽已關閉。")
    print("程式安全退出。")