import os
import re
import time
import requests
import pandas as pd
from typing import Optional, List, Dict
from io import StringIO
from datetime import datetime, timedelta
import pathlib
import sys

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 設定與路徑 ---
# ⚠️ 注意: 輸出目錄現在是動態的，將是 {OUTPUT_BASE_DIR}/{stock_no}
OUTPUT_BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "1_STOCK_DAY")
BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"

# 股票清單檔案路徑 (請務必確認此路徑在您的環境中是正確的)
# 依照您提供的路徑範例，假設 stocks_all.csv 位於 datas/raw/
STOCKS_ALL_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw", "stocks_all.csv")

# 交易日清單檔案路徑 (用於決定抓取截止日期)
CSV_FILE_PATH = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-2025.csv"

# --- 輔助函數：日期清單、檔案路徑檢查 (延用先前邏輯) ---

def get_latest_end_date() -> datetime.date:
    """根據現在時間 (21:00 前/後) 確定抓取的截止日期 (昨天/今天)。"""
    now = datetime.now()
    cutoff_time = datetime.strptime("21:00:00", "%H:%M:%S").time()
    
    if now.time() >= cutoff_time:
        return now.date()
    else:
        return (now - timedelta(days=1)).date()

def get_month_list_from_start_to_end(file_path: str) -> Optional[List[str]]:
    """
    1. 讀取 CSV 檔案內最早的交易日期。
    2. 根據截止日期，生成從起始月份到截止月份的所有月份清單 (YYYYMM)。
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        date_column = df.columns[0]
        all_dates_list = df[date_column].astype(str).str.strip().tolist()
        all_dates_list = sorted(list(set(all_dates_list)))

        if not all_dates_list:
            print("錯誤: 交易日清單中找不到任何日期數據。")
            return None

    except FileNotFoundError:
        print(f"錯誤: 找不到檔案 {file_path}，請確認路徑。")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理檔案 {file_path} 時發生錯誤: {e}")
        return None

    start_date_str = all_dates_list[0]
    end_date = get_latest_end_date()
    
    # 轉換為 datetime object
    start_dt = datetime.strptime(start_date_str, "%Y%m%d")
    end_dt = datetime(end_date.year, end_date.month, end_date.day)

    month_list = []
    current_dt = datetime(start_dt.year, start_dt.month, 1)

    while current_dt <= end_dt:
        # 抓取月份只需 YYYYMMDD 格式 (TWSE API 需要 YYYYMMDD 但只需月份即可)
        # 我們通常會選擇該月的第一天作為代表日期
        month_list.append(current_dt.strftime("%Y%m%d")) 
        
        # 移動到下個月
        if current_dt.month == 12:
            current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
        else:
            current_dt = current_dt.replace(month=current_dt.month + 1)

    print(f"\n--- 最終月份清單 (共 {len(month_list)} 個月) ---")
    print(f"起始月份: {month_list[0][:6]}")
    print(f"截止月份: {month_list[-1][:6]}")
    return month_list

def get_stock_list(file_path: str) -> Optional[List[str]]:
    """從 stocks_all.csv 讀取所有股票代號 (假設是第一個欄位，且為 4-6 位數字)。"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        # 假設股票代號在第一欄，並且確保代號是有效的數字格式
        stock_list = df.iloc[:, 0].astype(str).str.strip().tolist()
        
        # 簡單過濾：只保留長度為 4~6 的數字串
        filtered_stocks = [s for s in stock_list if re.fullmatch(r'\d{4,6}', s)]
        
        if not filtered_stocks:
            print(f"錯誤: 檔案 {file_path} 中找不到任何有效的股票代號。")
            return None
        
        print(f"--- 成功讀取 {len(filtered_stocks)} 個股票代號 ---")
        return filtered_stocks
    except FileNotFoundError:
        print(f"錯誤: 找不到股票清單檔案 {file_path}，請確認路徑是否為 D:\\Python_repo\\python\\Jason_Stock_Project\\datas\\raw\\stocks_all.csv")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理股票清單檔案 {file_path} 時發生錯誤: {e}")
        return None
        
def _check_folder_and_create(filepath: str):
    """檢查並建立所需的【資料夾】"""
    pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)

def _fetch_twse_data(url: str) -> Optional[str]:
    """嘗試從 TWSE 抓取資料，並返回原始文本。"""
    try:
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Big5'
        
        # 檢查 TWSE 回傳內容是否為錯誤訊息
        if "很抱歉" in response.text or "查無相關資料" in response.text:
            # 對於 STOCK_DAY，這通常表示該股票該月無交易，這是正常的
            return None
        
        return response.text
        
    except requests.exceptions.HTTPError as errh:
        # 404/400 等，也視為無資料
        return None 
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

def _read_twse_csv(response_text: str, header_row: int = 1) -> Optional[pd.DataFrame]:
    """將 TWSE 返回的 STOCK_DAY 文本解析為 Pandas DataFrame。"""
    try:
        data = StringIO(response_text)
        # STOCK_DAY 的 CSV 格式通常表頭在索引 1
        df = pd.read_csv(data, 
                         header=header_row, 
                         encoding='utf-8-sig', 
                         skipinitialspace=True,
                         engine='python',
                         on_bad_lines='skip' 
        )
        if not df.empty:
            df.columns = df.columns.str.strip() # 清理欄位名稱
            
            # 移除所有內容為空的欄位
            df.dropna(axis=1, how='all', inplace=True)
            
            if df.empty:
                return None
            
            # 清理：移除證券代號或日期為空的行 (通常是尾部的註解或空行)
            if '日期' in df.columns:
                 df = df[df['日期'].astype(str).str.strip() != '']
                
            return df
        return None

    except Exception as e:
        # print(f"❌ 解析 CSV 數據時發生錯誤: {e}") # 僅在嚴重錯誤時印出，避免洗版
        return None


# --- 核心單次抓取函數 ---

def fetch_twse_stock_day_single(target_date: str, stock_no: str) -> Optional[pd.DataFrame]:
    """
    抓取指定月份 (以該月第一個交易日表示) 和股票代號的 STOCK_DAY 報告。

    Args:
        target_date: 該月份的代表日期，格式為 YYYYMMDD。
        stock_no: 股票代號。

    Returns:
        成功時返回 DataFrame，失敗時返回 None。
    """
    
    # 輸出路徑 (包含股票代號)
    output_dir = os.path.join(OUTPUT_BASE_DIR, stock_no)
    month_str = target_date[:6]
    # 檔名使用月份和股票代號
    filename = os.path.join(output_dir, f"{month_str}_{stock_no}_STOCK_DAY.csv") 
    
    _check_folder_and_create(filename) # 確保 {OUTPUT_BASE_DIR}/{stock_no} 目錄存在

    # 檢查檔案是否已存在，存在則直接返回 None (表示【跳過】)
    if os.path.exists(filename):
        # ℹ️ 注意: 這裡不印出訊息，避免單次執行時，大量已存在檔案的訊息洗版
        return "EXISTS" # 使用特殊字串表示已存在

    # 構造 URL
    url = f"{BASE_URL}?date={target_date}&stockNo={stock_no}&response=csv"
    
    # 1. 抓取數據
    response_text = _fetch_twse_data(url)
    if response_text is None: 
        return None # 無數據或請求失敗
    
    # 2. 解析數據 (STOCK_DAY 報表頭部通常在索引 1)
    df = _read_twse_csv(response_text, header_row=1) 

    if df is not None and not df.empty:
        
        # 3. 儲存資料
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            # 成功抓取和儲存時，印出訊息
            print(f"  ✅ {stock_no} | {month_str} 資料儲存成功。")
            return df
        except Exception as e:
            print(f"❌ {stock_no} | {month_str} 資料儲存失敗: {e}")
            return None 
    else:
        # 該月無交易資料或解析失敗，返回 None
        return None


# --- 批次處理主函數 (多維度抓取) ---

def batch_fetch_twse_stock_day(month_list: List[str], stock_list: List[str]):
    """
    針對提供的月份清單和股票代號清單，進行批次抓取。
    """
    print("\n--- 開始批次抓取 TWSE STOCK_DAY 資料 (月份 x 股票代號) ---")
    
    total_tasks = len(month_list) * len(stock_list)
    tasks_processed = 0
    tasks_successful = 0
    tasks_skipped = 0
    
    for stock_no in stock_list:
        print(f"\n--- 🔄 開始處理股票代號: {stock_no} ---")
        
        for target_date in month_list:
            tasks_processed += 1
            month_str = target_date[:6]
            is_successful_or_skipped = False
            
            # 檢查檔案是否存在 (先檢查以跳過延遲)
            output_dir = os.path.join(OUTPUT_BASE_DIR, stock_no)
            filename = os.path.join(output_dir, f"{month_str}_{stock_no}_STOCK_DAY.csv")
            
            if os.path.exists(filename):
                tasks_skipped += 1
                is_successful_or_skipped = True
                # print(f"  ℹ️ {stock_no} | {month_str} 資料已存在，跳過。")
                continue # 立即跳到下一個月份，不執行延遲
            
            # 檔案不存在，執行抓取和重試邏輯
            max_attempts = 4
            for attempt in range(1, max_attempts + 1):
                
                print(f"  -> 嘗試抓取 {stock_no} | {month_str} (第 {attempt} 次)...")
                df_result = fetch_twse_stock_day_single(target_date, stock_no)
                
                # --- 🔑 關鍵修改在這裡 ---
                # 1. 檢查是否成功返回 DataFrame
                if isinstance(df_result, pd.DataFrame): 
                    tasks_successful += 1
                    is_successful_or_skipped = True
                    break  # 成功，跳出重試迴圈
                # 2. 檢查是否返回 "EXISTS" (應該在前面被跳過，但作為重試迴圈的退出條件)
                elif df_result == "EXISTS":
                    tasks_skipped += 1
                    is_successful_or_skipped = True
                    break
                # 抓取失敗 (df_result is None)
                if attempt < max_attempts:
                    delay_hours = attempt
                    # 測試環境用:
                    delay_seconds = delay_hours * 5 

                    print(f"🚨 {stock_no} | {month_str} 抓取失敗。將在 {delay_seconds} 秒後重試...")
                    time.sleep(delay_seconds)
                else:
                    # 超過最大嘗試次數
                    print(f"❌ {stock_no} | {month_str} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此月份。")
                    is_successful_or_skipped = True
                    break # 跳出重試迴圈
            
            # 只有在進行了網路抓取嘗試之後，才需要等待 2 秒
            if is_successful_or_skipped:
                # print("等待 2 秒後，準備處理下一個日期...")
                time.sleep(2)
    
    print("\n--- 🏁 批次抓取結束 ---")
    print(f"總任務數: {total_tasks}")
    print(f"成功儲存任務數: {tasks_successful}")
    print(f"跳過 (已存在) 任務數: {tasks_skipped}")
    print(f"失敗任務數: {total_tasks - tasks_successful - tasks_skipped}")


# --- 執行範例 ---

if __name__ == "__main__":
    
    # 1. 獲取月份清單
    month_list = get_month_list_from_start_to_end(CSV_FILE_PATH)
    
    # 2. 獲取股票代號清單
    stock_list = get_stock_list(STOCKS_ALL_CSV)
    
    if month_list and stock_list:
        # 3. 執行批次抓取
        batch_fetch_twse_stock_day(month_list, stock_list)
    else:
        print("無法取得完整的月份或股票清單，程式結束。")