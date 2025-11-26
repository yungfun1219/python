#標準函式庫
import os
import sys
import re
import shutil
from io import StringIO
import pathlib     # as pathlib
from datetime import date, datetime, timedelta, time as time_TimeClass

#第三方函式庫
import numpy as np # 用於數值操作
import pandas as pd # 用於資料處理與分析
import requests
import schedule
import keyboard  # 用於監聽鍵盤事件
from dotenv import load_dotenv # ➊ 匯入函式庫
from typing import Optional, Tuple, List, Union, Dict, Any
import time as time_module # 用於 sleep() 或 time()

#本地模組
import get_stocks_company_all 
from utils import jason_utils as jutils

# 假設 get_previous_n_trading_days 和 find_last_n_trading_days_with_time_check 已經定義

# ----------------------------------------------------------------------------------
def determine_target_trading_date(
    file_path: pathlib.Path,
    n_days: int = 6
) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    根據當前時間判斷當日是否為交易日或數據是否已更新，
    並獲取最近的 N 個交易日清單。

    Args:
        file_path (pathlib.Path): 交易日 CSV 檔案的路徑。
        n_days (int): 需要往前追溯的交易日數量。

    Returns:
        Tuple[Optional[str], Optional[List[str]]]: 
        (TARGET_DATE_YYYYMMDD, result_found_days_list) 
        - TARGET_DATE_YYYYMMDD: 最終決定抓取資料的日期 (YYYYMMDD 格式)。
        - result_found_days_list: N 個交易日組成的列表 (YYYY/MM/DD 格式)，如果失敗則為 None。
    """
    NOW_DATETIME = datetime.now()
    DATE_TO_CHECK = NOW_DATETIME.strftime("%Y/%m/%d")  # 今天日期：YYYY/MM/DD
    DATE_TO_CHECK_NOW_STR = NOW_DATETIME.strftime("%Y/%m/%d %H:%M:%S")

    # 1. 嘗試以當前時間檢查交易日清單
    print(f"\n[定時任務]: 嘗試以當前時間 {DATE_TO_CHECK_NOW_STR} 檢查最近交易日 (N={n_days})。")
    # 假設 get_previous_n_trading_days 的 n_days 參數控制返回的列表長度
    result_found_days = get_previous_n_trading_days(str(file_path), DATE_TO_CHECK_NOW_STR, n_days=n_days)
    
    # 2. 如果第一次檢查失敗 (返回 None)，則嘗試使用前一天的時間點來檢查
    if result_found_days is None or len(result_found_days) < 1:
        print("\n[時間檢查]: 第一次檢查失敗 (可能 CSV 錯誤或數據不連續)。")
        
        # 轉換到昨天的時間點字串
        YESTERDAY_DATETIME = NOW_DATETIME - timedelta(days=1)
        DATE_TO_CHECK_YESTERDAY_STR = YESTERDAY_DATETIME.strftime("%Y/%m/%d %H:%M:%S")
        print(f"[時間檢查]: 嘗試以昨天時間 {DATE_TO_CHECK_YESTERDAY_STR} 重新檢查。")
        
        result_found_days = get_previous_n_trading_days(str(file_path), DATE_TO_CHECK_YESTERDAY_STR, n_days=n_days)

    # 3. 檢查最終結果：如果還是 None 或列表為空，則返回失敗
    if result_found_days is None or len(result_found_days) == 0:
        print("\n[定時任務]: 嚴重錯誤：無法取得任何有效的交易日清單，無法繼續執行。")
        return None, None 
    
    # 4. 根據交易日清單的最新日期決定 TARGET_DATE
    LATEST_TRADING_DAY_YMD = result_found_days[-1] # YYYY/MM/DD 格式
    TARGET_DATE_YYYYMMDD = LATEST_TRADING_DAY_YMD.replace('/', '') # YYYYMMDD 格式

    if DATE_TO_CHECK == LATEST_TRADING_DAY_YMD:
        # 今天是交易日，且已經過了截止時間 (由 get_previous_n_trading_days 內邏輯判斷)
        print(f"\n[時間檢查]: 今天日期 ({DATE_TO_CHECK}) 為交易日，抓取當天資料 ({TARGET_DATE_YYYYMMDD})。")
    else:
        # 今天是休市日，或尚未過截止時間，抓取前一個交易日
        print(f"\n[時間檢查]: 今天 ({DATE_TO_CHECK}) 非最新交易日，抓取前一個交易日 ({LATEST_TRADING_DAY_YMD}) 的資料。")

    print("\n" + "="*50)
    print("--- 程式開始執行：TWSE 報告資料抓取 ---")
    print("="*50 + "\n")
    
    return TARGET_DATE_YYYYMMDD, result_found_days
# ----------------------------------------------------------------------------------

def main_run():
    #----------------
    global running # 引用全局變數
    
    if not running:
        print("\n[定時任務]: 偵測到退出信號，跳過本次執行。")
        return

    # --- 區塊 1: 日期與交易日判斷 (優化後) ---
    NOW_DATETIME = datetime.now()
    Now_day_time = NOW_DATETIME.strftime("%Y-%m-%d %H:%M")  # 取得目前系統時間的日期及時間
    Now_time_year = NOW_DATETIME.strftime("%Y")             # 取得目前系統時間的「年」
    
    Trading_day_file_path = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-{Now_time_year}.csv"
    
    # 決定要抓取的目標日期 (TARGET_DATE) 並獲取最近交易日清單
    # TARGET_DATE: YYYYMMDD (目標抓取日期)
    # result_trading_days_list: [YYYY/MM/DD, ...] (N個交易日清單)
    TARGET_DATE, result_trading_days_list = determine_target_trading_date(
        file_path=Trading_day_file_path, 
        n_days=6
    )

    # 如果獲取交易日清單失敗，安全退出
    if TARGET_DATE is None:
        print("\n[定時任務]: 核心日期判斷失敗，程式中止。")
        return
    
    # 確保有交易日清單，用於後續的 find_last_n_trading_days_with_time_check
    # 由於 find_last_n_trading_days_with_time_check 仍需要 DataFrame 格式，我們暫時維持其在下面被呼叫
    
    time_module.sleep(2) 
    
    # --------------------------------------------------------------------------
    # --- 原有程式碼區塊 2: 檔案抓取部分開始 ---
    # --------------------------------------------------------------------------
    
    # 這裡的 target_info = _get_target_date_and_month() 
    # 應該要使用我們剛剛確定的 TARGET_DATE (YYYYMMDD) 來覆寫或初始化 daily_date 和 monthly_date。
    # 為了保持程式碼的可運行性，我將假設您會修改 _get_target_date_and_month 讓它使用 TARGET_DATE。
    
    # 獲取單一目標日期和月份 (需要根據 TARGET_DATE 進行調整，這裡假設已調整)
    target_info = _get_target_date_and_month(TARGET_DATE) # 假設修改 _get_target_date_and_month 接受 TARGET_DATE
    daily_date = target_info["daily_date"]
    monthly_date = target_info["monthly_date"]
    start_time = target_info["start_time"]
    
    # ... (後續的 fetch 任務和分析邏輯保持不變) ...
    
    # ⚠️ 注意：您需要確保下面這行使用的 recent_trading_days_df 能夠成功運作，
    # 由於您原先的程式碼是呼叫了 find_last_n_trading_days_with_time_check 函式來取得 DataFrame
    # 這裡我們需要使用 result_trading_days_list 來替代或傳入。
    
    # 假設 find_last_n_trading_days_with_time_check 的功能與 determine_target_trading_date 重複
    # 建議直接將 result_trading_days_list 轉為 DataFrame (如果需要)
    # 否則，維持您原有的呼叫，但要確保它不會失敗。
    
    # 由於 determine_target_trading_date 已經為我們找到了清單，
    # 這裡建議使用這個清單，而不是再次呼叫可能失敗的函式。
    
    # **建議替代方案 (減少重複邏輯):**
    if result_trading_days_list is not None:
        recent_trading_days_df = pd.DataFrame({'日期': result_trading_days_list})
    else:
        # 如果 result_trading_days_list 是 None，這裡會是 None
        recent_trading_days_df = None 

    if recent_trading_days_df is None:
        print("【嚴重錯誤】無法取得最近交易日 DataFrame，跳過後續分析。")
        return
        
    # ... (後續的庫存股和關注股分析邏輯) ...
    
    # ... (最後的 Line 通知發送邏輯) ...