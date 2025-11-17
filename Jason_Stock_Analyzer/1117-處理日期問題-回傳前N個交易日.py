import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Union, List

# 根據指定日期與時間（21:00截止）提供往前6個交易日，前一個交易日則為df[-1]
def get_previous_n_trading_days(
    file_path: str,
    datetime_to_check: str,
    n_days: int = 6,        # 往前提出6個交易日
    CUTOFF_HOUR: int = 21,  # 設定截止時間為 21:00
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
    
    # 檢查檔案路徑
    if not os.path.exists(file_path):
        print(f"【錯誤】檔案路徑不存在，請確認路徑是否正確: {file_path}")
        return None
        
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。欄位有: {df.columns.tolist()}")
            return None
            
        # 確保日期欄位是字串，以避免格式不一致的問題
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
        # 如果時間在 21:00 (含) 之後，有效日期為今天；否則為前一天。
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

        max_lookback_days = n_days * 3  # 設定最大追溯天數，避免無限迴圈
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

        # 將列表反轉，使其按時間順序排列 (如果需要的話，通常是從最早到最近)
        # 如果希望從最近到最舊，則不需要反轉
        #trading_days_found.reverse() 

        # 4. 判斷今天是否為交易日並回傳結果
        current_day_is_trading = df[date_column_name].isin([input_date.strftime(input_date_format)]).any()
        
        if current_day_is_trading:
             print(f"\n今天日期 ({input_date.strftime(input_date_format)}) 為交易日。")
        else:
             print(f"\n今天日期 ({input_date.strftime(input_date_format)}) 為休市日。")
        
        
        # 確保列表是從最舊到最新排列
        trading_days_found.reverse()

        if len(trading_days_found) == n_days:
            # 完整收集到 N 天
            print(f"✅ 成功收集到 {n_days} 個交易日。")
            return trading_days_found
        else:
            # 未收集到 N 天 (通常是數據不足)
            print(f"⚠️ 僅找到 {len(trading_days_found)} 個交易日，數量不足 {n_days} 個。")
            return trading_days_found # 即使不足也回傳找到的結果

    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return None

# =======================================================================
# --- 範例執行 ---
# =======================================================================

# 模擬一個包含足夠交易日期的 CSV 檔案
# 模擬一個包含交易日期的 CSV 檔案
HOLIDAYS_FILE = r'D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays\trading_day_2021-2025.csv'


# # 假設的交易日清單 (包含足夠多的交易日，以便追溯 6 天)
# test_trading_days = [
#     '2025/09/30', '2025/10/01', '2025/10/02', '2025/10/03', # 3, 4(Sat), 5(Sun)
#     '2025/10/06', '2025/10/07', '2025/10/08', '2025/10/09', 
#     '2025/10/10', # 星期五
#     '2025/10/13', '2025/10/14', '2025/10/15'
# ]

# # 模擬檔案創建
# try:
#     if not os.path.exists(HOLIDAYS_FILE):
#         df_holidays = pd.DataFrame({'日期': test_trading_days, '備註': ['交易日'] * len(test_trading_days)})
#         df_holidays.to_csv(HOLIDAYS_FILE, index=False, encoding='utf-8-sig')
#         print(f"【初始化】已創建測試檔案: {HOLIDAYS_FILE}")
# except Exception as e:
#     print(f"【初始化錯誤】創建測試檔案失敗: {e}")
    
# print("\n" + "="*80)

# 測試案例 1：今天 (2025/10/10, 週五) 22:00 (超過 21:00 截止線)
# 預期結果：從 10/10 開始向前找 6 個交易日: [10/03, 10/06, 10/07, 10/08, 10/09, 10/10]
print("--- 案例 1: 交易日 22:00 (從當天開始追溯 6 天) ---")
result1 = get_previous_n_trading_days(
    file_path=HOLIDAYS_FILE, 
    datetime_to_check='2025/10/10 22:00:00',
    n_days=6
)
print(f"最終結果 (6 個交易日): {result1}") 

print("\n" + "="*80)

# 測試案例 2：今天 (2025/10/10, 週五) 15:30 (未達 21:00 截止線)
# 預期結果：從 10/09 開始向前找 6 個交易日: [10/02, 10/03, 10/06, 10/07, 10/08, 10/09]
print("--- 案例 2: 交易日 15:30 (從前一天開始追溯 6 天) ---")
result2 = get_previous_n_trading_days(
    file_path=HOLIDAYS_FILE, 
    datetime_to_check='2025/10/10 15:30:00',
    n_days=6
)
print(f"最終結果 (6 個交易日): {result2}") 

print("\n" + "="*80)

# 測試案例 3：休市日 (2025/10/11, 週六) 10:00
# 預期結果：有效查詢日期是 10/11，但 10/11 是休市日。
# 追溯過程會從 10/11 -> 10/10 -> 10/09 -> ...
# 找到的 6 天應與案例 1 相同: [10/03, 10/06, 10/07, 10/08, 10/09, 10/10]
print("--- 案例 3: 休市日 10:00 (從休市日開始追溯 6 天) ---")
result3 = get_previous_n_trading_days(
    file_path=HOLIDAYS_FILE, 
    datetime_to_check='2025/10/11 10:00:00',
    n_days=6
)
print(f"最終結果 (6 個交易日): {result3}")


# 測試案例 5：今天+現在
Now_time_hour = datetime.now().strftime("%H")  #取得目前系統時間的「幾點鐘」
Now_day_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")  #取得目前系統時間的日期及時間「例如 2025-11-12 11:12」
    
print(f"--- 案例 4: 今天{datetime.now().strftime("%Y/%m/%d") }，現在時間{datetime.now().strftime("%H:%M:%S")} ---")
result4 = get_previous_n_trading_days(
    file_path=HOLIDAYS_FILE, 
    datetime_to_check=datetime.now().strftime("%Y/%m/%d %H:%M:%S")
)
print(f"最終結果: {result4}")
print(f"前一個交易日: {result4[-1]}")