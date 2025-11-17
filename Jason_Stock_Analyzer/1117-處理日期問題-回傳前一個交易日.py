import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Union, Tuple


# 檢查指定日期與時間是否為交易日，並根據時間（21:00截止）回傳有效的交易日。
def get_effective_trading_date(
    file_path: str,
    datetime_to_check: str,
    date_column_name: str = '日期'
) -> Union[str, None]:
    """
    檢查指定日期與時間是否為交易日，並根據時間（21:00截止）回傳有效的交易日。
    Args:
        file_path (str): 包含交易日清單的 CSV 檔案完整路徑 (假設檔案中列出的是交易日)。
        datetime_to_check (str): 要檢查的日期和時間字串，例如 '2025/10/10 15:30:00'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[str, None]: 格式為 'YYYY/MM/DD' 的有效交易日字串，或發生錯誤時回傳 None。
    """    
    # 設定截止時間為 21:00
    CUTOFF_HOUR = 21
    
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

        # 1. 解析輸入的日期時間
        # 允許的輸入格式，如果輸入格式與此不同，會拋出 ValueError
        input_dt_format = '%Y/%m/%d %H:%M:%S'
        input_date_format = '%Y/%m/%d'
        
        try:
            input_dt = datetime.strptime(datetime_to_check, input_dt_format)
        except ValueError:
            print(f"【錯誤】輸入日期時間格式不正確。應為 '{input_dt_format}'。您輸入的是: {datetime_to_check}")
            return None
        
        input_date = input_dt.date()
        input_time = input_dt.time()
        
        # 2. 根據時間判斷「有效檢查日期」
        # 如果時間在 21:00 (含) 之後，有效日期為今天；否則為前一天。
        
        cutoff_time = input_dt.replace(hour=CUTOFF_HOUR, minute=0, second=0, microsecond=0).time()
        
        effective_check_date = input_date
        date_label = "今天"
        
        if input_time < cutoff_time:
            # 如果在 21:00 之前，視為前一天的交易
            effective_check_date = input_date - timedelta(days=1)
            date_label = "昨天"

        print(f"輸入日期時間: {datetime_to_check}")
        print(f"截止時間 ({CUTOFF_HOUR}:00) 前後判斷結果：取 {date_label} ({effective_check_date.strftime(input_date_format)}) 或更早的交易日。")
        
        # 3. 迴圈向前尋找最近的交易日
        current_check_date = effective_check_date
        
        while True:
            date_str = current_check_date.strftime(input_date_format)
            
            # 使用 isin 檢查日期是否存在於交易日清單中
            is_trading_day = df[date_column_name].isin([date_str]).any()
            
            if is_trading_day:
                print(f"✅ 找到有效交易日: {date_str}")
                # 找到交易日，回傳結果
                return date_str
            
            # 如果不是交易日，則繼續往前推一天
            print(f"❌ {date_str} 非交易日，繼續尋找前一個交易日...")
            current_check_date -= timedelta(days=1)

            # 加入安全機制，防止無限迴圈 (例如，如果日期倒退太多，可能代表資料有問題)
            # 這裡簡單設定最多往前找 30 天
            if effective_check_date - current_check_date > timedelta(days=30):
                print("【警告】已向前追溯超過 30 天，可能資料清單不完整。停止尋找。")
                return None

    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return None

# =======================================================================
# --- 範例執行 ---
# =======================================================================

# 模擬一個包含交易日期的 CSV 檔案
HOLIDAYS_FILE = r'D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays\trading_day_2021-2025.csv'

# 假設的交易日清單（2025/10/10 是星期五，2025/10/11 & 12 是週末）
# test_trading_days = [
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
    
# print("\n" + "="*50)

# # 測試案例 1：今天 (2025/10/10, 週五) 22:00 (超過 21:00 截止線)
# # 預期結果：今天 (2025/10/10) 為交易日，時間在 21:00 之後，回傳當天日期。
# print("--- 案例 1: 交易日 22:00 (回傳當天) ---")
# result1 = get_effective_trading_date(
#     file_path=HOLIDAYS_FILE, 
#     datetime_to_check='2025/10/10 22:00:00'
# )
# print(f"最終結果: {result1}") 

# print("\n" + "="*50)

# # 測試案例 2：今天 (2025/10/10, 週五) 15:30 (未達 21:00 截止線)
# # 預期結果：時間在 21:00 之前，有效日期為昨天 2025/10/09，回傳 2025/10/09。
# print("--- 案例 2: 交易日 15:30 (回傳前一個交易日) ---")
# result2 = get_effective_trading_date(
#     file_path=HOLIDAYS_FILE, 
#     datetime_to_check='2025/10/10 15:30:00'
# )
# print(f"最終結果: {result2}") 

# print("\n" + "="*50)

# # 測試案例 3：休市日 (2025/10/11, 週六) 10:00
# # 預期結果：10/11 是休市日，往前找交易日 -> 10/10。
# print("--- 案例 3: 休市日 10:00 (回傳前一個交易日) ---")
# result3 = get_effective_trading_date(
#     file_path=HOLIDAYS_FILE, 
#     datetime_to_check='2025/10/11 10:00:00'
# )
# print(f"最終結果: {result3}") 

# print("\n" + "="*50)

# # 測試案例 4：連假後 (2025/10/13, 週一) 10:00 (未達 21:00 截止線)
# # 預期結果：時間在 21:00 之前，有效日期為昨天 10/12 (休市日)，往前找交易日 -> 10/10。
# print("--- 案例 4: 交易日 10:00 (前一天是週末，回傳更早的交易日) ---")
# result4 = get_effective_trading_date(
#     file_path=HOLIDAYS_FILE, 
#     datetime_to_check='2025/10/13 10:00:00'
# )
# print(f"最終結果: {result4}")

# 測試案例 5：今天+現在
Now_time_hour = datetime.now().strftime("%H")  #取得目前系統時間的「幾點鐘」
Now_day_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")  #取得目前系統時間的日期及時間「例如 2025-11-12 11:12」
    
print(f"--- 案例 5: 今天{datetime.now().strftime("%Y/%m/%d") }，現在時間{datetime.now().strftime("%H:%M:%S")} ---")
result5 = get_effective_trading_date(
    file_path=HOLIDAYS_FILE, 
    datetime_to_check=datetime.now().strftime("%Y/%m/%d %H:%M:%S")
)
print(f"最終結果: {result5}")



# print(f"--- 案例 5: 今天{datetime.now().strftime("%Y/%m/%d") }，現在時間{datetime.now().strftime("%H:%M:%S")} ---")
# result6 = get_effective_trading_date(
#     file_path=HOLIDAYS_FILE, 
#     datetime_to_check=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# )
# print(f"最終結果: {result6}")