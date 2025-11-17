import requests
import pandas as pd
import re
import os # 雖然大部分替換，但有些舊程式碼可能仍會用到 os，暫時保留
import time
from io import StringIO
from typing import Optional, Union
from datetime import datetime, timedelta
import warnings
from pathlib import Path # 引入 Pathlib

# 忽略 requests 啟用 verify=False 時可能出現的 SSL 警告
warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 參數設定 ---
# 每次重試的延遲時間 (秒)
RETRY_DELAY_SECONDS = 3600 # 1 小時
# 最大重試次數
MAX_RETRIES = 5 
# 假設此腳本會被放置在 Jason_Stock_Analyzer 目錄下，或者在同一層級
# 使用 Pathlib 定義基礎路徑
BASE_DIR = Path(__file__).resolve().parent

# --- 輔助函式 ---

def check_folder_and_create(filepath: Path):
    """
    檢查路徑中的資料夾是否存在，不存在則創建。
    參數 filepath 必須為 pathlib.Path 物件。
    """
    output_dir = filepath.parent
    if not output_dir.exists():
        # parents=True 確保創建所有上層目錄，exist_ok=True 避免重複創建錯誤
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"創建資料夾: {output_dir}")

def _read_twse_csv(response_text: str, header_row: int = 1) -> Optional[pd.DataFrame]:
    """
    將 TWSE 回傳的 CSV 文字內容解析為 Pandas DataFrame。
    """
    try:
        csv_file = StringIO(response_text)
        # T86 表格的欄位名稱通常在回傳的 CSV 內容的第二行
        # skipfooter=1 是因為 TWSE CSV 最後一行通常是總計或其他無用資訊
        df = pd.read_csv(csv_file, header=header_row, skipfooter=1, engine='python')
        
        # 清除欄位名稱前後的空白
        df.columns = df.columns.str.strip()
        
        return df

    except Exception as e:
        print(f"❌ 解析 CSV 內容時發生錯誤: {e}")
        return None

def _fetch_twse_data(url: str) -> Optional[str]:
    """
    獲取 TWSE 資料，使用 Big5 編碼解碼。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # verify=False 是為了避免 SSL 驗證問題 (但應謹慎使用)
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        # TWSE 網站 CSV 內容通常是 Big5 編碼
        response.encoding = 'Big5'
        return response.text
    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP 錯誤：{errh} (該日可能無交易資料，代碼: {response.status_code})")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")

    return None

# --- 核心抓取函式 (單次嘗試) ---

def fetch_twse_t86(target_date: str) -> Optional[pd.DataFrame]:
    """
    抓取指定日期的 T86 報告 (三大法人買賣超彙總表 - 依類別)，並儲存為 CSV。
    
    :param target_date: 查詢日期，格式為 YYYYMMDD (例如: 20251031)
    :return: 包含三大法人買賣超資料的 DataFrame，如果失敗則為 None
    """
    
    if not re.fullmatch(r'\d{8}', target_date): 
        print("日期格式錯誤，請使用 YYYYMMDD 格式。")
        return None
        
    # 定義 URL 結構 (不變)
    base_url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    url = f"{base_url}?date={target_date}&selectType=ALL&response=csv"
    
    # 【Pathlib 修改點 1】：使用 Path 物件建構路徑
    OUTPUT_DIR = BASE_DIR / "datas" / "raw" / "11_T86"
    filename: Path = OUTPUT_DIR / f"{target_date}_T86_InstitutionalTrades.csv"

    # 【Pathlib 修改點 2】：傳入 Path 物件給 check_folder_and_create
    check_folder_and_create(filename)
    
    print(f"-> 嘗試從 TWSE 獲取資料...")
    
    # 1. 抓取資料
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # 2. 解析 CSV (不變)
    df = _read_twse_csv(response_text, header_row=1)

    # 3. 數據清理與儲存
    if df is not None and '證券代號' in df.columns:
        # 清除沒有證券代號的空行
        df = df[df['證券代號'].astype(str).str.strip() != '']
        
        # 確保數字欄位可以被轉換，清除總計行等
        numeric_cols = [col for col in df.columns if '買賣超' in col or '金額' in col]
        if numeric_cols:
            for col in numeric_cols:
                 # 清除逗號後轉換為數字，不能轉換的設為 NaN
                df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
            # 刪除所有數字欄位皆為 NaN 的行 (可能是合計或無用訊息)
            df.dropna(subset=numeric_cols, how='all', inplace=True)
            
            # 如果數據清理後仍有資料
            if not df.empty:
                # 儲存 CSV (Pandas to_csv 支援 Path 物件)
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"✅ {filename} 儲存成功，共 {len(df)} 筆資料。")
                return df
    
    print(f"❌ 數據處理失敗，可能該日期 ({target_date}) 為非交易日或網站資料結構改變。")
    return None

# --- 重試機制主函數 ---

def fetch_t86_with_retry(target_date: str) -> Optional[pd.DataFrame]:
    """
    嘗試抓取 T86 資料，失敗時等待 1 小時後重試。
    """
    retries = 0
    while retries < MAX_RETRIES:
        print(f"\n--- 嘗試抓取 T86 數據 (日期: {target_date}, 第 {retries + 1} 次嘗試) ---")
        
        df = fetch_twse_t86(target_date)
        
        # 判斷是否成功
        if df is not None and not df.empty:
            print(f"✅ 資料抓取與儲存流程成功完成！")
            return df
        
        retries += 1
        
        if retries < MAX_RETRIES:
            delay_hours = RETRY_DELAY_SECONDS // 3600
            print(f"⚠️ 抓取失敗，等待 {delay_hours} 小時後重試 (下次重試時間: {datetime.now() + timedelta(seconds=RETRY_DELAY_SECONDS)})。")
            time.sleep(RETRY_DELAY_SECONDS)
        else:
            print(f"❌ 已達最大重試次數 ({MAX_RETRIES})，放棄抓取。請手動檢查日期或網站狀態。")
            return None
    return None


# --- 範例執行區 ---
if __name__ == "__main__":
    # 設定您想要抓取的日期
    # 注意：請使用一個有效的交易日 (例如：最近的交易日)
    
    # 範例日期：假設今天 (2025/11/14) 是最近的交易日
    # 由於我們沒有實際的交易日曆，這裡使用一個固定的未來日期作為測試
    TARGET_DATE = "20251105" # 請替換為您想抓取的日期 YYYYMMDD
    
    print(f"==================================================")
    print(f"開始執行 T86 抓取程式，目標日期: {TARGET_DATE}")
    print(f"最大重試次數: {MAX_RETRIES}, 重試間隔: {RETRY_DELAY_SECONDS // 3600} 小時")
    print(f"==================================================")
    
    # 開始抓取流程
    result_df = fetch_t86_with_retry(TARGET_DATE)
    
    if result_df is not None:
        print("\n--- 成功抓取並儲存的數據 (前 5 筆) ---")
        print(result_df.head())
    else:
        print("\n--- 抓取流程最終失敗 ---")