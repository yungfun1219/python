import pandas as pd
import pathlib
import re
from typing import Union, List, Tuple
from datetime import date, datetime, time, timedelta
import os

# --- 參數設定 ---
# 股票資料檔案的路徑基礎
BASE_STOCK_DATA_PATH = pathlib.Path(r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\20251114_T86_InstitutionalTrades.csv")
FILE_NAME_TEMPLATE = "_{}_T86_InstitutionalTrades.csv" # 檔案名稱模板
VOLUME_COL = "三大法人買賣超股數"
CODE_COL = "證券代號"
N_DAYS = 5
TRADING_DAY_FILE = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / "trading_day_2021-2025.csv"

# --- 輔助函式：取得最近 N 個交易日 ---
# 保持您原有的函式邏輯，但為了整合，我們讓它直接返回日期清單
def find_last_n_trading_days_with_time_check(file_path: pathlib.Path, n: int = 5) -> Union[List[str], None]:
    """
    從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。
    :return: 包含最近 N 個交易日日期字串 (YYYY/MM/DD) 的清單，或 None。
    """
    now = datetime.now()
    today_date = now.date()
    cutoff_time = time(15, 0, 0)
    is_after_cutoff = now.time() >= cutoff_time

    print(f"當前日期: {today_date.strftime('%Y/%m/%d')}, 當前時間是否在 15:00 之後: {is_after_cutoff}")
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except Exception as e:
        print(f"❌ 錯誤：無法讀取交易日檔案 {file_path}: {e}")
        return None

    date_column = '日期' 
    if date_column not in df.columns and 'Date' in df.columns:
        date_column = 'Date'
    elif date_column not in df.columns:
        print(f"❌ 錯誤：交易日檔案中找不到日期欄位。")
        return None
        
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.normalize()
    df.dropna(subset=[date_column], inplace=True)
    all_trading_dates = set(df[date_column].dt.date)
    is_today_trading_day = today_date in all_trading_dates
    
    print(f"今天 ({today_date.strftime('%Y/%m/%d')}) 是否為交易日: {is_today_trading_day}")

    inclusion_date = today_date - timedelta(days=1)
    
    if is_today_trading_day and is_after_cutoff:
        inclusion_date = today_date
        print("-> 判斷：納入今天的交易日。")
    else:
        inclusion_date = today_date - timedelta(days=1)
        print("-> 判斷：排除今天的交易日，只取昨天及更早的日期。")

    df_past = df[df[date_column].dt.date <= inclusion_date]
    df_past = df_past.sort_values(by=date_column, ascending=False)
    last_n_days = df_past.head(n)

    if last_n_days.empty:
        print(f"⚠️ 警告：交易日資料不足，無法找到往前 {n} 個交易日。")
        return None

    # 返回 YYYY/MM/DD 格式的清單
    date_list = last_n_days[date_column].dt.strftime('%Y/%m/%d').tolist()
    
    print(f"\n✅ 成功找到今天往前 {n} 個交易日。")
    return date_list

# --- 核心篩選函式 (與前版相同，用於讀取 T86 檔案) ---
def find_positive_institutional_trades(file_path: pathlib.Path, volume_col: str, code_col: str) -> pd.DataFrame:
    """讀取、清洗並找出買超且代號為 4 碼的股票。"""
    
    try:
        try:
            df = pd.read_csv(file_path, encoding='big5')
        except UnicodeDecodeError:
             df = pd.read_csv(file_path, encoding='utf-8')
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    required_cols = [volume_col, code_col, '證券名稱']
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame()

    try:
        df[volume_col] = df[volume_col].astype(str).str.replace('"', '', regex=False).str.replace(',', '', regex=False).str.strip()
        df[volume_col] = pd.to_numeric(df[volume_col], errors='coerce')
        df[code_col] = df[code_col].astype(str).str.strip()
        df.dropna(subset=[volume_col], inplace=True)
    except Exception:
        return pd.DataFrame()

    # 1. 篩選代號為 4 碼數字
    df_filtered_code = df[df[code_col].str.match(r'^\d{4}$')]
    if df_filtered_code.empty:
        return pd.DataFrame()

    # 2. 篩選買超 > 0
    df_positive = df_filtered_code[df_filtered_code[volume_col] > 0].copy()
    if df_positive.empty:
        return pd.DataFrame()
        
    # 3. 排序
    df_sorted = df_positive.sort_values(by=volume_col, ascending=False)
    
    return df_sorted

# --- 主控函數：查詢多日資料 ---
def main_get_recent_buy_in_data() -> str:
    """
    查詢最近 N 個交易日的買超前 30 名股票，並彙整成清單字串。
    """
    final_report = "📈 近期三大法人買超清單 (前 {} 名)：\n\n".format(N_DAYS * 30)
    all_dates_data = []
    
    # 1. 取得最近 N 個交易日清單
    recent_dates = find_last_n_trading_days_with_time_check(TRADING_DAY_FILE, N_DAYS)
    
    if recent_dates is None:
        return "❌ 錯誤：無法取得最近交易日清單，請檢查 trading_day 檔案路徑和內容。"
    
    # 將日期由舊到新排序 (雖然 find_last_n_trading_days_with_time_check 已經這樣做了)
    recent_dates.sort() 
    
    # 2. 迭代每個日期，進行股票篩選
    for date_str_slash in recent_dates:
        # 將 YYYY/MM/DD 轉換為 YYYYMMDD 構造檔名
        date_str_clean = date_str_slash.replace('/', '')
        file_name = date_str_clean + FILE_NAME_TEMPLATE.format(date_str_clean)
        file_path = BASE_STOCK_DATA_PATH / file_name
        
        print(f"\n處理檔案: {file_path.name}")
        
        # 呼叫核心篩選函式
        buy_in_df = find_positive_institutional_trades(file_path, VOLUME_COL, CODE_COL)
        
        # 檢查是否有數據
        if buy_in_df.empty:
            all_dates_data.append(f"----- 【日期: {date_str_slash}】 ❌ 無買超或檔案遺失 -----\n")
            continue

        # 3. 取出前 30 名數據
        TOTAL_STOCKS_TO_DISPLAY = 30
        top_30_display = buy_in_df.head(TOTAL_STOCKS_TO_DISPLAY).copy()
        
        # 4. 格式化輸出
        
        top_30_display['買超張數'] = top_30_display[VOLUME_COL].apply(
            lambda x: f"{int(x/1000):,}"
        )
        
        # 構造單日報告標題
        report_title = f"----- 【日期: {date_str_slash}】 買超前 {len(top_30_display)} 名 -----\n"
        
        # 構造清單字串 (代號 | 證券名稱 | 買超張數)
        daily_list = ""
        for _, rol in top_30_display.iterrows():
            code = rol[CODE_COL]
            # 確保名稱被清理和填充
            name = rol['證券名稱'].strip().ljust(8) 
            volume = rol['買超張數']
            
            daily_list += f"{code} | {name} | ({volume}張)\n"

        all_dates_data.append(report_title + daily_list)

    # 5. 彙總所有日期的報告
    final_report += "\n".join(all_dates_data)
    final_report += "\n\n=== 報告結束 ==="
    
    return final_report

# --- 執行主函數並輸出結果 ---
if __name__ == "__main__":
    report = main_get_recent_buy_in_data()
    print(report)