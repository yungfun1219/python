import pandas as pd
from datetime import date, datetime, time, timedelta
import os

def find_last_n_trading_days_with_time_check(file_path, n=5):
    """
    從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。

    :param file_path: 股票交易日 CSV 檔案路徑
    :param n: 往前找的交易日數量 (預設為 5)
    :return: 包含最近 N 個交易日的 DataFrame (或 None if failed)
    """
    
    # 1. 定義當前時間和判斷標準
    now = datetime.now()
    today_date = now.date()
    cutoff_time = time(15, 0, 0) # 下午 15:00:00
    is_after_cutoff = now.time() >= cutoff_time

    print(f"當前日期: {today_date.strftime('%Y/%m/%d')}, 當前時間是否在 15:00 之後: {is_after_cutoff}")
    
    # 2. 讀取交易日檔案
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_path}")
        return None
    except Exception as e:
        print(f"讀取檔案時發生錯誤，請檢查檔案路徑和編碼: {e}")
        return None

    # 假設日期欄位為 '日期'
    date_column = '日期' 
    if date_column not in df.columns:
        # 嘗試使用常見的英文欄位名
        if 'Date' in df.columns:
            date_column = 'Date'
        else:
            print(f"錯誤：無法識別交易日期的欄位名稱。請檢查您的 CSV 檔案。")
            return None
        
    # 3. 清理和轉換日期格式
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.normalize()
    df.dropna(subset=[date_column], inplace=True)
    
    # 建立所有交易日的集合，用於快速判斷今天是否為交易日
    all_trading_dates = set(df[date_column].dt.date)
    is_today_trading_day = today_date in all_trading_dates
    
    print(f"今天 ({today_date.strftime('%Y/%m/%d')}) 是否為交易日: {is_today_trading_day}")

    # 4. 根據時間判斷決定資料篩選的截止日期
    
    # 預設：如果不滿足納入今天的條件，則截止日期為昨天
    inclusion_date = today_date - timedelta(days=1)
    
    # 判斷是否應該納入今天
    if is_today_trading_day and is_after_cutoff:
        # 條件 1: 今天是交易日
        # 條件 2: 且時間在 15:00 之後 (視為今天交易已完成)
        # -> 納入今天
        inclusion_date = today_date
        print("-> 判斷：納入今天的交易日。")
    else:
        # 其他情況 (非交易日、或交易日但未滿 15:00)
        # -> 排除今天，只取昨天及更早的交易日
        inclusion_date = today_date - timedelta(days=1)
        print("-> 判斷：排除今天的交易日，只取昨天及更早的日期。")

    # 5. 篩選、排序並選取最近 N 個交易日
    
    # 篩選出日期小於或等於決定截止日期的交易日
    df_past = df[df[date_column].dt.date <= inclusion_date]
    
    # 確保日期由近到遠排序
    df_past = df_past.sort_values(by=date_column, ascending=False)

    # 選取最近的 N 個交易日
    last_n_days = df_past.head(n)

    if last_n_days.empty:
        print(f"警告：交易日資料不足，無法找到往前 {n} 個交易日。")
        return None

    # 將結果由舊到新排序並格式化輸出
    last_n_days = last_n_days.sort_values(by=date_column, ascending=True)
    last_n_days[date_column] = last_n_days[date_column].dt.strftime('%Y/%m/%d')
    
    print(f"\n✅ 成功找到今天往前 {n} 個交易日。")
    return last_n_days

# --- 執行部分 ---
file_path = r'D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays\trading_day_2021-2025.csv'
N_DAYS = 5 # 往前找的交易日數量

recent_trading_days_df = find_last_n_trading_days_with_time_check(file_path, n=N_DAYS)

if recent_trading_days_df is not None:
    print("\n--- 最近 5 個交易日 (含日期、星期、假日說明欄位) ---")
    print(recent_trading_days_df["日期"])
    
    # 由於您的 CSV 只有交易日，若要包含「星期」和「假日說明」
    # 則需要您的 trading_day_2021-2025.csv 檔案中包含這些欄位。
    # 否則，最終輸出將只包含 CSV 原始欄位。