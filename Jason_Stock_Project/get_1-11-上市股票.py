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

# --- 全域設定與路徑 ---

CODE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS_ALL_CSV = os.path.join(CODE_DIR, "datas", "raw", "stocks_all.csv")
# 交易日清單檔案路徑 (僅用於輔助判斷今日是否為交易日)
CSV_FILE_PATH = os.path.join(CODE_DIR, "datas", "processed", "get_holidays", "trading_day_2021-2025.csv")

# 新增：用於嘗試讀取 CSV 檔案的編碼清單
ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950'] 


# --- 輔助函數 ---

# 修正：載入並預處理交易日清單，新增多編碼嘗試
def _load_trading_days(file_path: str) -> Optional[List[str]]:
    """
    從 CSV 檔案讀取所有交易日，並返回排序好的 YYYYMMDD 字串清單。
    增加多種編碼嘗試，以解決讀取 CSV 檔案的致命錯誤。
    """
    df = None
    used_encoding = None

    # 嘗試多種編碼讀取檔案
    for encoding in ENCODINGS_TO_TRY:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            used_encoding = encoding
            print(f"--- ℹ️ 交易日清單檔案 {os.path.basename(file_path)} 成功以 {encoding} 編碼讀取。")
            break  # 讀取成功，跳出迴圈
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"致命錯誤：找不到交易日清單檔案 {file_path}。")
            return None
        except Exception as e:
            print(f"致命錯誤：處理交易日清單時發生非編碼錯誤: {e}")
            return None
            
    if df is None or df.empty:
        print("致命錯誤：嘗試所有編碼後，交易日清單檔案仍無法讀取或為空。")
        return None
    
    try:
        date_column = df.columns[0]
        
        trading_days_ymd = []
        # 清理並轉換日期格式
        for date_str in df[date_column].astype(str).str.strip().tolist():
            try:
                # 嘗試使用 YYYY/MM/DD 格式解析
                dt_obj = datetime.strptime(date_str, "%Y/%m/%d").date()
            except ValueError:
                try:
                    # 嘗試使用 YYYYMMDD 格式解析
                    dt_obj = datetime.strptime(date_str, "%Y%m%d").date()
                except ValueError:
                    continue 
            
            # 將所有日期統一轉換為 YYYYMMDD 字串格式
            trading_days_ymd.append(dt_obj.strftime("%Y%m%d"))
            
        all_trading_days = sorted(list(set(trading_days_ymd)))
        return all_trading_days

    except Exception as e:
        # 捕捉後續資料處理 (非檔案讀取) 的錯誤
        print(f"致命錯誤：交易日數據解析或處理時發生錯誤: {e}")
        return None


# 函式：從預先載入的清單中找到【前一個交易日】。
def _get_previous_trading_day(all_trading_days: List[str], current_date: datetime.date) -> Optional[datetime.date]:
    """
    從預先載入的交易日清單中找到當前日期的【前一個交易日】。
    """
    if not all_trading_days:
        return None

    current_date_str = current_date.strftime("%Y%m%d")
    
    # 找到所有比今天日期小的交易日
    previous_trading_days = [
        d for d in all_trading_days if d < current_date_str
    ]
    
    if previous_trading_days:
        # 返回其中最大的一個 (即最近的一個交易日)
        return datetime.strptime(previous_trading_days[-1], "%Y%m%d").date()
    else:
        print("⚠️ 警告：交易日清單中找不到前一個交易日。")
        return None
    
# 檢查並建立所需的【資料夾】
def _check_folder_and_create(filepath: str):
    
    pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)

# 函式：根據 21:00 基準判斷目標日期，並接受交易日清單。
def _get_target_date_and_month(all_trading_days: Optional[List[str]]) -> Dict[str, Optional[str]]:
    """
    根據 21:00 基準判斷目標日期，並使用傳入的交易日清單。
    """
    now = datetime.now()
    cutoff_time = datetime.strptime("21:00:00", "%H:%M:%S").time()
    
    target_date_dt = None # 預設為 None
    
    if now.time() >= cutoff_time:
        # 情況 1: 21:00 之後 -> 抓取今天 (實際日期)
        target_date_dt = now.date()
        print(f"【日期判斷】{now.strftime('%H:%M:%S')} 晚於 21:00，目標日為今天 ({target_date_dt.strftime('%Y/%m/%d')})。")

    else:
        # 情況 2: 21:00 之前 -> 抓取前一個交易日
        if all_trading_days is None:
             print("❌ 錯誤：交易日清單為空，無法確定前一個交易日。")
             return {
                "daily_date": None,
                "monthly_date": None,
                "start_time": now.strftime('%H:%M:%S')
            }
        
        # 從預先載入的清單中找到最近的交易日 (避免在函式內部讀取檔案)
        previous_trading_day_dt = _get_previous_trading_day(all_trading_days, now.date())

        if previous_trading_day_dt:
            target_date_dt = previous_trading_day_dt
            print(f"【日期判斷】{now.strftime('%H:%M:%S')} 早於 21:00，目標日為前一個交易日 ({target_date_dt.strftime('%Y/%m/%d')})。")
        else:
            print("❌ 錯誤：無法確定前一個交易日，所有日報任務將跳過。")
            return {
                "daily_date": None,
                "monthly_date": None,
                "start_time": now.strftime('%H:%M:%S')
            }

    # 紀錄開始的時間          
    start_time = now.strftime('%H:%M:%S')
 
    # 針對日報：
    final_daily_date = target_date_dt.strftime("%Y%m%d")
    
    # 針對 STOCK_DAY：只需要目標日期所在月份的代表日期 (YYYYMM01)
    current_month_date = target_date_dt.strftime("%Y%m") + "01"

    print(f"【最終目標】日報抓取日期: {final_daily_date}")
    print(f"【最終目標】STOCK_DAY月份: {current_month_date[:6]}")
    print(f"【最終目標】開始時間: {start_time}")

    return {
        "daily_date": final_daily_date,
        "monthly_date": current_month_date,
        "start_time": start_time
    }

# 從 stocks_all.csv 讀取股票清單，並依據【市場別】欄位篩選出「上市」公司。
def get_stock_list(file_path: str) -> Optional[List[str]]:

    try:
        # 讀取整個 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 1. 尋找【市場別】欄位
        market_col = None
        for col in df.columns:
            if "市場別" in col or "市場" in col or "類別" in col:
                market_col = col
                break
        
        if market_col is None:
            # 警告：如果找不到市場別欄位，則退回到只抓取 4 位數字的代號
            print("警告：找不到包含 '市場別' 或 '市場' 字樣的欄位，將退回僅篩選 4 位數字代號。")
            
            stock_list = df.iloc[:, 0].astype(str).str.strip().tolist()
            filtered_stocks = [
                s for s in stock_list 
                if re.fullmatch(r'^\d{4}$', s)
            ]
            
        else:
            # 2. 找到市場別欄位，進行篩選
            
            # 清理市場別欄位的字串，並篩選出包含「上市」字樣的行
            df_listed = df[
                df[market_col].astype(str).str.strip().str.contains("上市", na=False)
            ].copy() 
            
            # 3. 取得第一欄的股票代號
            stock_list = df_listed.iloc[:, 0].astype(str).str.strip().tolist()
            
            # 額外檢查：確保代號是有效的數字格式（通常是 4 位純數字）
            filtered_stocks = [s for s in stock_list if re.fullmatch(r'\d{4,6}', s)]
            
            
        if not filtered_stocks:
            print("錯誤: 依據「市場別」篩選後，找不到任何符合條件的上市公司代號。")
            return None
            
        print(f"--- 成功依據「市場別」篩選，讀取 {len(filtered_stocks)} 個上市公司代號 ---")
        # print(filtered_stocks) # 註釋掉避免輸出過長列表
        return filtered_stocks
    except pd.errors.EmptyDataError:
        print(f"錯誤: 股票清單檔案 {file_path} 為空。")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理股票清單檔案 {file_path} 時發生錯誤: {e}")
        return None

# 將 TWSE 返回的文本解析為 Pandas DataFrame。
def _read_twse_csv(response_text: str, header_row: int = 1, first_col_name: Optional[str] = None) -> Optional[pd.DataFrame]:
    
    try:
        data = StringIO(response_text)
        df = pd.read_csv(data, 
                         header=header_row, 
                         encoding='utf-8', 
                         skipinitialspace=True,
                         engine='python',
                         on_bad_lines='skip' 
        )
        if not df.empty:
            df.columns = df.columns.str.strip()
            df.dropna(axis=1, how='all', inplace=True)
            
            if df.empty: return None
            
            if first_col_name in df.columns:
                 df = df[df[first_col_name].astype(str).str.strip() != '']
                
            return df
        return None
    except Exception as e:
        return None


# --- 通用日報抓取主函數 (僅抓取當日) ---
def fetch_single_daily_report(
    target_date: str, 
    base_url: str, 
    output_folder_num: str,
    output_filename_suffix: str,
    url_params: str = "",
    first_col_name: Optional[str] = None,
    header_row: int = 1
):
    """
    抓取單日報表數據的通用函數，具備檔案存在即跳過的機制。
    """
    
    OUTPUT_DIR = os.path.join(CODE_DIR, "datas", "raw", output_folder_num)
    filename = os.path.join(OUTPUT_DIR, f"{target_date}{output_filename_suffix}.csv")
    _check_folder_and_create(filename)

    print(f"\n--- 🚀 處理 {output_folder_num} ({target_date}) ---")

    # 1. 檢查檔案是否已存在
    if os.path.exists(filename):
        print(f"  ℹ️ {target_date} 資料已存在，跳過。")
        return # 跳過，不執行延遲

    # 2. 檔案不存在，開始執行抓取和重試
    is_successful = False
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        
        url = f"{base_url}?date={target_date}{url_params}&response=csv"
        
        # 執行抓取
        response_text = _fetch_twse_data(url)
        df = None
        if response_text is not None:
            df = _read_twse_csv(response_text, header_row=header_row, first_col_name=first_col_name)

        if df is not None:
            # 成功儲存
            try:
                df.to_csv(filename, index=False, encoding='utf-8')
                print(f"  ✅ {target_date} 資料儲存成功。")
                is_successful = True
                break
            except Exception as e:
                print(f"❌ {target_date} 資料儲存失敗: {e}")
                break

        # 失敗處理
        if attempt < max_attempts:
            delay_seconds = attempt * 5 
            print(f"🚨 抓取失敗 (第 {attempt} 次)。將在 {delay_seconds} 秒後重試...")
            time.sleep(delay_seconds)
        else:
            print(f"❌ {target_date} 資料經過 {max_attempts} 次嘗試後仍然失敗。")
            break
    
    # 3. 只有在進行了網路抓取或重試之後，才需要等待 2 秒
    if is_successful or attempt == max_attempts:
        time.sleep(2)

def _fetch_twse_data(url: str) -> Optional[str]:
    """嘗試從 TWSE 抓取資料，並返回原始文本。"""
    try:
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Utf-8'
        
        if "很抱歉" in response.text or "查無相關資料" in response.text:
            return None
        
        return response.text
        
    except requests.exceptions.HTTPError:
        return None 
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

# --- 任務 1: STOCK_DAY 獨立處理 (當月/覆蓋) ---
def fetch_twse_stock_day_single_month(month_date: str, stock_list: List[str]):

    print(f"\n--- 🚀 開始 STOCK_DAY 抓取 ({month_date[:6]}) (將直接覆蓋) ---")
    
    OUTPUT_BASE_DIR = os.path.join(CODE_DIR, "datas", "raw", "1_STOCK_DAY")
    BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
    
    tasks_successful = 0
    tasks_failed = 0
    
    for stock_no in stock_list:
        output_dir = os.path.join(OUTPUT_BASE_DIR, stock_no)
        month_str = month_date[:6]
        filename = os.path.join(output_dir, f"{month_str}_{stock_no}_STOCK_DAY.csv") 
        _check_folder_and_create(filename)
        
        is_successful = False
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            
            url = f"{BASE_URL}?date={month_date}&stockNo={stock_no}&response=csv"
            
            # 1. 抓取數據
            response_text = _fetch_twse_data(url)
            df = None
            if response_text is not None:
                df = _read_twse_csv(response_text, header_row=1, first_col_name='日期') 
            
            if df is not None and not df.empty:
                # 2. 儲存資料 (直接覆蓋)
                try:
                    df.to_csv(filename, index=False, encoding='utf-8')
                    print(f"  ✅ {stock_no} | {month_str} 資料已覆蓋儲存。")
                    tasks_successful += 1
                    is_successful = True
                    break
                except Exception as e:
                    print(f"❌ {stock_no} | {month_str} 資料儲存失敗: {e}")
                    tasks_failed += 1
                    break
            
            # 抓取失敗 (df is None)
            if attempt < max_attempts:
                delay_seconds = attempt * 5 
                print(f"🚨 {stock_no} | {month_str} 抓取失敗。將在 {delay_seconds} 秒後重試...")
                time.sleep(delay_seconds)
            else:
                print(f"❌ {stock_no} | {month_str} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此股票。")
                tasks_failed += 1
                break
                
            # 3. 每次嘗試網路請求後，等待 2 秒 (無論成功或失敗)
        if is_successful or attempt == max_attempts:
            time.sleep(2)
            
    print(f"\n--- 🏁 STOCK_DAY 抓取結束。成功覆蓋: {tasks_successful}, 失敗: {tasks_failed} ---")
# --- 主執行函數 ---

def main():
    
    # ... (前段載入和日期計算保持不變) ...
    
    # --- 1. 獨立執行：預載入交易日清單 ---
    print("--- ⏱️ 載入交易日清單 ---")
    all_trading_days = _load_trading_days(CSV_FILE_PATH)
    
    # --- 2. 獨立執行：計算目標日期和月份 ---
    target_info = _get_target_date_and_month(all_trading_days)
    daily_date = target_info["daily_date"]
    monthly_date = target_info["monthly_date"]
    start_time = target_info["start_time"]
    # -- 單獨抓取
    daily_date = "20251128"
    monthly_date = "202511"
    
    # --- 3. 獨立執行：獲取【股票代號】清單 ---
    stock_list = get_stock_list(STOCKS_ALL_CSV)
    
    
    if daily_date is None:
        print("\n--------------------------------------")
        print("⚠️ 由於無法確定目標交易日，所有任務已跳過。")
        print("--------------------------------------")
    else:
        # ... (中間的抓取任務呼叫保持不變) ...

        # --- B. 處理 STOCK_DAY (任務 1 - 當月覆蓋) ---
        print("\n--- 📝 開始執行 STOCK_DAY 覆蓋任務 (B) ---")
        if stock_list and monthly_date:
            fetch_twse_stock_day_single_month(monthly_date, stock_list)
        elif not stock_list:
            print("警告：無法取得股票清單 (stocks_all.csv)，跳過 STOCK_DAY 抓取。")
        elif not monthly_date:
            print("警告：無法取得目標月份，跳過 STOCK_DAY 抓取。")
            
    # ===================================================
    # 💥 最終總結區塊 (新增目標資訊) 💥
    # ===================================================
    print("\n======================================")
    print("✅ 所有 TWSE 數據抓取任務已完成。")
    
    # 輸出目標日期和月份 (如果存在)
    if daily_date:
        print(f"【日報日期】{daily_date}")
        print(f"【月份覆蓋】{monthly_date[:6]}")
    
    # 輸出股票數量
    if stock_list:
        print(f"【股票總數】{len(stock_list)} 檔上市公司")
        
    print(f"【開始時間】{start_time} ")
    print(f"【完成時間】{datetime.now().strftime('%H:%M:%S')} ")
    print("======================================")
    
if __name__ == "__main__":
    main()