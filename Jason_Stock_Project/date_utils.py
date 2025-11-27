import datetime
from datetime import date, datetime, timedelta, time as time_TimeClass
import pathlib
from typing import Optional, Tuple, List, Union, Dict, Any
import os
import pandas as pd # 用於資料處理與分析


# ----------------------------------------------------
# --- 輔助函式: 根據時間截斷向前追溯交易日 ---
# ----------------------------------------------------

def get_previous_n_trading_days(
    file_path: str,
    datetime_to_check: str,
    n_days: int = 6, # 往前提出6個交易日
    CUTOFF_HOUR: int = 21, # 設定截止時間為 21:00
    date_column_name: str = '日期') -> Union[List[str], None]:
    """
    根據指定日期與時間（21:00截止）確定一個有效查詢日期，
    並從該日期（含）開始向前追溯 N 個最近的交易日。
    Args:
        file_path (str): 包含交易日清單的 CSV 檔案完整路徑 (假設檔案中列出的是交易日)。
        datetime_to_check (str): 要檢查的日期和時間字串，例如 '2025/10/10 15:30:00'。
        n_days (int): 要獲取的上一個交易日的數量 (預設為 6)。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[List[str], None]: 包含 N 個交易日字串（'YYYY/MM/DD' 格式）的列表，
                                 或發生錯誤時回傳 None。
    """
    
    # 檢查檔案路徑 (此處 file_path 應為 str)
    if not os.path.exists(file_path):
        print(f"【錯誤】檔案路徑不存在，請確認路徑是否正確: {file_path}")
        return None
        
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='big5')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。欄位有: {df.columns.tolist()}")
            return None
            
        # 確保日期欄位是字串
        df[date_column_name] = df[date_column_name].astype(str)

        # 設定日期格式
        input_dt_format = '%Y/%m/%d %H:%M:%S'
        input_date_format = '%Y/%m/%d'
        
        # 1. 解析輸入的日期時間
        try:
            input_dt = datetime.strptime(datetime_to_check, input_dt_format)
        except ValueError:
            print(f"【錯誤】輸入日期時間格式不正確。應為 '{input_dt_format}'。您輸入的是: {datetime_to_check}")
            return None
        
        input_date = input_dt.date()
        input_time = input_dt.time()
        
        # 2. 根據時間判斷「有效查詢日期」
        cutoff_time = input_dt.replace(hour=CUTOFF_HOUR, minute=0, second=0, microsecond=0).time()
        
        effective_check_date = input_date
        
        if input_time < cutoff_time:
            # 如果在 21:00 之前，視為前一天的交易
            effective_check_date = input_date - timedelta(days=1)
        
        # 3. 迴圈向前尋找最近的 N 個交易日
        current_check_date = effective_check_date
        trading_days_found: List[str] = []
        
        print(f"輸入日期時間: {datetime_to_check}")
        print(f"起始查詢日期 (根據 {CUTOFF_HOUR}:00 截止線判斷): {current_check_date.strftime(input_date_format)}")
        print(f"目標：向前追溯 {n_days} 個交易日...")

        max_lookback_days = n_days * 3 
        days_passed = 0

        while len(trading_days_found) < n_days:
            
            # 安全機制檢查
            if days_passed > max_lookback_days:
                print(f"【警告】已向前追溯超過 {max_lookback_days} 天 ({current_check_date.strftime(input_date_format)})，可能資料清單不完整。停止尋找。")
                break

            date_str = current_check_date.strftime(input_date_format)
            
            # 使用 isin 檢查日期是否存在於交易日清單中
            is_trading_day = df[date_column_name].isin([date_str]).any()
            
            if is_trading_day:
                # 找到交易日，添加到列表
                trading_days_found.append(date_str)
                print(f"✅ 找到第 {len(trading_days_found)} 個交易日: {date_str}")
            
            # 無論是否為交易日，都往前推一天，直到找到足夠的數量
            current_check_date -= timedelta(days=1)
            days_passed += 1

        # 將列表反轉，使其按時間順序排列 (從最早到最近)
        trading_days_found.reverse()

        # 4. 判斷今天是否為交易日並回傳結果
        current_day_is_trading = df[date_column_name].isin([input_date.strftime(input_date_format)]).any()
        
        if current_day_is_trading:
            print(f"\n今天日期 ({input_date.strftime(input_date_format)}) 為交易日。")
        else:
            print(f"\n今天日期 ({input_date.strftime(input_date_format)}) 為休市日。")
        
        
        if len(trading_days_found) == n_days:
            print(f"✅ 成功收集到 {n_days} 個交易日。")
            return trading_days_found
        elif len(trading_days_found) > 0:
            print(f"⚠️ 僅找到 {len(trading_days_found)} 個交易日，數量不足 {n_days} 個。")
            return trading_days_found 
        else:
            print(f"【警告】在追溯的 {max_lookback_days} 天內，未找到任何交易日。")
            return None # 確保回傳 None

    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return None


# ----------------------------------------------------
# --- 主要函式: 獲取報告目標日期 ---
# ----------------------------------------------------

def get_trading_day_info(trading_day_file_path: Union[pathlib.Path, str]) -> Dict[str, Optional[str]]:
    """
    處理日期和交易日判斷邏輯，決定目標抓取日期 (TARGET_DATE)。
    
    Args:
        trading_day_file_path: 交易日曆檔案的路徑 (e.g., .../trading_day_2025.csv)。
        
    Returns:
        Dict[str, Optional[str]]: 包含各種格式化日期和目標日期的字典。
    """
    
    # 1. 獲取當前時間和格式化
    NOW_DATETIME = datetime.now()
    Now_day_time = NOW_DATETIME.strftime("%Y-%m-%d %H:%M")  # 系統時間 (2025-11-27 11:53)
    Now_time_year = NOW_DATETIME.strftime("%Y") 
    
    DATE_TO_CHECK = NOW_DATETIME.strftime("%Y/%m/%d")  # 今天日期 (2025/11/27)
    DATE_TO_CHECK_NOW = NOW_DATETIME.strftime("%Y/%m/%d %H:%M:%S") # 帶秒數的時間
    
    TARGET_DATE_STR = None # 最終要抓取的日期
    
    # 2. 處理要抓取哪一天的資料邏輯 
    # ❗ 修正：將 pathlib.Path 轉為 str，確保與 get_previous_n_trading_days 參數型別一致
    file_path_str = str(trading_day_file_path) 
    
    # 嘗試以當前時間點檢查最近的交易日
    result_found_days = get_previous_n_trading_days(file_path_str, DATE_TO_CHECK_NOW)
    
    # 如果初次檢查失敗，則退一天再檢查
    if result_found_days is None:
        DATE_TO_CHECK_PREV = NOW_DATETIME - timedelta(days=1) 
        DATE_TO_CHECK_NOW_PREV = DATE_TO_CHECK_PREV.strftime("%Y/%m/%d %H:%M:%S") 
        
        print(f"\n[定時任務]: 首次檢查失敗，嘗試回退至前一天 ({DATE_TO_CHECK_NOW_PREV}) 檢查。")
        result_found_days = get_previous_n_trading_days(file_path_str, DATE_TO_CHECK_NOW_PREV)
    
    # 3. 根據檢查結果決定目標日期
    if result_found_days:
        
        # 交易日清單的最後一個日期，是最近的交易日
        most_recent_trading_day = result_found_days[-1]
        
        if DATE_TO_CHECK == most_recent_trading_day: # 如果今天是交易日
            TARGET_DATE_STR = most_recent_trading_day
            print(f"\n[時間檢查]: 今天日期 ({DATE_TO_CHECK}) 為交易日。")
            print(f"[時間檢查]: 現在時間為 {DATE_TO_CHECK_NOW}，抓取 ({TARGET_DATE_STR}) 當天資料。")
        else:
            # 今天不是交易日，或者時間未到 (例如，下午九點前，前一天的資料更完整)
            TARGET_DATE_STR = most_recent_trading_day 
            print(f"\n[時間檢查]: 現在時間為 {DATE_TO_CHECK_NOW}，當天資料尚未更新/非交易日，將提供前一個交易日 ({TARGET_DATE_STR}) 的資料。")
    
    # 4. 格式化輸出
    if TARGET_DATE_STR:
        daily_date = TARGET_DATE_STR.replace('/', '') # YYYYMMDD 格式 (日報)
        monthly_date = daily_date[:6] # YYYYMM 格式 (月報)
    else:
        # 如果無法確定目標日期 (例如：交易日曆檔案嚴重錯誤)
        daily_date = None
        monthly_date = None
        print("❌ 錯誤：無法確定目標交易日，數據抓取將跳過。")

    # 打印程式開始信息 
    print("\n" + "="*50)
    print("--- 程式開始執行：TWSE 報告資料抓取 ---")
    print("="*50 + "\n")
    
    return {
        "daily_date": daily_date,  # '20251127' (日報目標)
        "monthly_date": monthly_date,  # '202511' (月報目標)
        "target_date_slash": TARGET_DATE_STR,  # '2025/11/27'
        "now_day_time": Now_day_time,  # '2025-11-27 11:53'
        "start_time": NOW_DATETIME.strftime('%H:%M:%S'), # '11:53:12'
        "trading_day_year": Now_time_year, # '2025'
    }