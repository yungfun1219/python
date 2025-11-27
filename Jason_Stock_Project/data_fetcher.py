import time as time_module # 用於 sleep() 或 time()
import os, pandas, requests
import pathlib     # as pathlib
from typing import Optional, Tuple, List, Union, Dict, Any
from io import StringIO
import pandas as pd # 用於資料處理與分析
from datetime import date, datetime, timedelta, time as time_TimeClass


CODE_DIR = os.path.dirname(os.path.abspath(__file__))

def _fetch_twse_data(url: str) -> Optional[str]:
    """嘗試從 TWSE 抓取資料，並返回原始文本。"""
    try:
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Big5'
        
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

# 共用的輔助函式，用於處理 TWSE 的 Big5 編碼和 Pandas 讀取邏輯。
def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """
    Args:
        response_text: HTTP 請求回傳的文字內容 (Big5 編碼)。
        header_row: CSV 檔案中資料表頭所在的行數 (0-indexed)。
    Returns:
        Optional[pd.DataFrame]: 處理後的 DataFrame。
    """
    try:
        csv_data = StringIO(response_text)
        # 嘗試讀取 CSV
        df = pd.read_csv(
            csv_data, 
            header=header_row,          # 資料表頭所在的行數
            skipinitialspace=True,      # 跳過分隔符後的空格
            on_bad_lines='skip',        # 跳過格式不正確的行
            encoding='Big5'             # 使用 Big5 編碼讀取
        )
        # TWSE 的 CSV 欄位名稱常有隱藏空格，導致 df.columns 無法正確匹配。
        if not df.empty:
            df.columns = df.columns.str.strip()
        # 移除所有欄位皆為空的行
        df = df.dropna(how='all')
        # 移除資料尾部可能出現的彙總或備註行
        if not df.empty and df.iloc[-1].astype(str).str.contains('合計|總計|備註', na=False).any():
            df = df.iloc[:-1]
        return df
    except Exception as e:
        print(f"在讀取或清理 CSV 數據時發生錯誤: {e}")

        return None
    
# 抓取當月所有股票的 STOCK_DAY 資料，並直接覆蓋檔案。
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
        pathlib.Path(filename).parent.mkdir(parents=True, exist_ok=True)
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
                    df.to_csv(filename, index=False, encoding='big5')
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
                time_module.sleep(delay_seconds)
            else:
                print(f"❌ {stock_no} | {month_str} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此股票。")
                tasks_failed += 1
                break
                
        # 3. 每次嘗試網路請求後，等待 2 秒 (無論成功或失敗)
        if is_successful or attempt == max_attempts:
            time_module.sleep(2)
            
    print(f"\n--- 🏁 STOCK_DAY 抓取結束。成功覆蓋: {tasks_successful}, 失敗: {tasks_failed} ---")
# ==========================================================

# 從 stocks_all.csv 讀取股票清單，並依據【市場別】欄位篩選出「上市」公司。
def get_stock_list(file_path: str) -> Optional[List[str]]:

    try:
        # 讀取整個 CSV 檔案
        df = pd.read_csv(file_path, encoding='big5')
        
        # 1. 尋找【市場別】欄位
        # 嘗試從欄位名稱中找出包含 "市場別"、"類別" 或 "市場" 的欄位
        market_col = None
        for col in df.columns:
            if "市場別" in col or "市場" in col or "類別" in col:
                market_col = col
                break
        
        if market_col is None:
            # 警告：如果找不到市場別欄位，則退回到只抓取 4 位數字的代號（避免抓取全部）
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
            ].copy() # 使用 copy 避免 SettingWithCopyWarning
            
            # 3. 取得第一欄的股票代號
            # 假設股票代號是第一欄 (索引 0)
            stock_list = df_listed.iloc[:, 0].astype(str).str.strip().tolist()
            
            # 額外檢查：確保代號是有效的數字格式（通常是 4 位純數字）
            filtered_stocks = [s for s in stock_list if re.fullmatch(r'\d{4,6}', s)]
            
        
        if not filtered_stocks:
            print("錯誤: 依據「市場別」篩選後，找不到任何符合條件的上市公司代號。")
            return None
            
        print(f"--- 成功依據「市場別」篩選，讀取 {len(filtered_stocks)} 個上市公司代號 ---")
        print(filtered_stocks)
        return filtered_stocks
    except pd.errors.EmptyDataError:
        print(f"錯誤: 股票清單檔案 {file_path} 為空。")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理股票清單檔案 {file_path} 時發生錯誤: {e}")
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
    pathlib.Path(filename).parent.mkdir(parents=True, exist_ok=True)

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
                df.to_csv(filename, index=False, encoding='big5')
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
            time_module.sleep(delay_seconds)
        else:
            print(f"❌ {target_date} 資料經過 {max_attempts} 次嘗試後仍然失敗。")
            break
    
    # 3. 只有在進行了網路抓取或重試之後，才需要等待 2 秒
    if is_successful or attempt == max_attempts:
        time_module.sleep(2)
        
def fetch_all_twse_reports(daily_date: str, monthly_date: str, stock_list: list):
    """
    執行所有 TWSE 日報和月報的資料抓取任務。
    """
    print("="*50 + "\n--- 程式開始執行：TWSE 報告資料抓取 ---")
    
    if daily_date is None:
        print("⚠️ 由於無法確定目標交易日，日報任務已跳過。")
    else:
        print(f"--- A. 處理單日報表 ({daily_date}) ---")
        
        # 定義所有日報任務的清單
        # 結構: (url, file_prefix, folder_suffix, first_col, header_row, url_params)
        DAILY_REPORTS = [
            ("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX", "2_MI_INDEX", "_MI_INDEX_Sector", "項目", 2, None),
            ("https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d", "3_BWIBBU_d", "_BWIBBU_d_IndexReturn", "產業別", 1, None),
            ("https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU", "5_TWTASU", "_TWTASU_VolumePrice", "項目", 1, None),
            ("https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU", "6_BFIAMU", "_BFIAMU_DealerTrade", "自營商", 1, None),
            ("https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK", "7_FMTQIK", "_FMTQIK_BrokerVolume", "券商", 1, None),
            ("https://www.twse.com.tw/rwd/zh/fund/BFI82U", "8_BFI82U", "_BFI82U_3IParty_Day", "項目", 1, "&type=day&dayDate"),
            ("https://www.twse.com.tw/rwd/zh/fund/TWT43U", "9_TWT43U", "_TWT43U_ForeignTrade", "外資及陸資", 1, None),
            ("https://www.twse.com.tw/rwd/zh/fund/TWT44U", "10_TWT44U", "_TWT44U_InvestmentTrust", "投信", 1, None),
            ("https://www.twse.com.tw/rwd/zh/fund/T86", "11_T86", "_T86_InstitutionalTrades", "證券代號", 1, "&selectType=ALL"),
            # TWT92U 融資融券 (如果需要再啟用)
            # ("https://www.twse.com.tw/rwd/zh/marginTrading/TWT92U", "4_TWT92U", "_TWT92U_Margin", "股票代號", 1, None),
        ]
        
        for url, prefix, suffix, first_col, header_row, url_params in DAILY_REPORTS:
            fetch_single_daily_report(
                daily_date, url, prefix, suffix, 
                first_col_name=first_col, 
                header_row=header_row, 
                url_params=url_params
            )
            time_module.sleep(1) # 增加間隔，避免被鎖

    # --- 處理 STOCK_DAY (月報) ---
    if stock_list and monthly_date:
        print(f"--- B. 處理 STOCK_DAY (任務 1 - {monthly_date} 覆蓋) ---")
        fetch_twse_stock_day_single_month(monthly_date, stock_list)
    elif not stock_list:
        print("警告：無法取得股票清單 (stocks_all.csv)，跳過 STOCK_DAY 抓取。")
    elif not monthly_date:
        print("警告：無法取得目標月份，跳過 STOCK_DAY 抓取。")

    print("\n======================================")
    print("✅ TWSE 數據抓取任務已完成。")
    print("======================================")

