import os
import re
import time
import requests
import pandas as pd
from typing import Optional, List
from io import StringIO
import sys


# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 設定與路徑 ---
# ⚠️ 請確保 'datas/raw/2_MI_INDEX' 路徑存在或可被建立
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "2_MI_INDEX")
BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"


# --- 輔助函數 (為使程式碼可執行而加入) ---
# 檢查並建立所需的【資料夾】
def _check_folder_and_create(filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

def _fetch_twse_data(url: str) -> Optional[str]:
    """嘗試從 TWSE 抓取資料，並返回原始文本。"""
    try:
        # 設置 User-Agent 以模擬瀏覽器行為
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            response = requests.get(url, headers=headers, verify=False, timeout=10)
            response.raise_for_status() 
            response.encoding = 'Big5'
            return response.text
        except requests.exceptions.HTTPError as errh:
            print(f"❌ HTTP 錯誤：{errh} (該日可能無交易資料)")
        except requests.exceptions.RequestException as err:
            print(f"❌ 連線或 Requests 錯誤: {err}")
        except Exception as e:
            print(f"❌ 發生其他錯誤: {e}")
        
        # 檢查 TWSE 回傳內容是否為錯誤訊息
        if "查詢日期大於今日" in response.text or "很抱歉" in response.text:
             print("⚠️ TWSE 網站返回錯誤訊息，該日可能無資料或日期超出範圍。")
             return None

        # 嘗試使用正確編碼解析，處理 CSV 檔案常見的 BOM
        response.encoding = 'utf-8-sig' 
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ 網路請求失敗或超時: {e}")
        return None

def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """將 TWSE 返回的文本解析為 Pandas DataFrame。"""
    try:
        data = StringIO(response_text)
        # header_row=1: MI_INDEX 報表實際的表頭在索引 1 (0-based)
        df = pd.read_csv(data, 
                        header=header_row, 
                        encoding='utf-8-sig', 
                        skipinitialspace=True,
                        engine='python',
                        on_bad_lines='skip' # <-- 這個參數會忽略欄位數不正確的行，避免程式崩潰
        )
        if not df.empty:
            df.columns = df.columns.str.strip() # 清理欄位名稱
            
            # 刪除結尾可能的空白行或備註行
            if '指數' in df.columns:
                 # 移除 '指數' 欄位為空字串或空白的行
                 df = df[df['指數'].astype(str).str.strip() != '']
                 
            # 移除所有內容為空的欄位 (如 CSV 結尾的空欄位)
            df.dropna(axis=1, how='all', inplace=True)
            
            if df.empty:
                print("⚠️ 解析 CSV 後 DataFrame 為空，可能無有效資料。")
                return None
                
            return df
        return None

    except Exception as e:
        print(f"❌ 解析 CSV 數據時發生錯誤: {e}")
        return None


# --- 核心單日抓取函數 (Refactored from user's snippet) ---

def fetch_twse_mi_index_single(target_date: str) -> Optional[pd.DataFrame]:
    """
    抓取指定日期的 MI_INDEX 報告。

    Args:
        target_date: 欲抓取的日期，格式為 YYYYMMDD。

    Returns:
        成功時返回 DataFrame，失敗時返回 None。
    """
    if not re.fullmatch(r'\d{8}', target_date): 
        print(f"日期格式錯誤: {target_date}")
        return None
        
    url = f"{BASE_URL}?date={target_date}&type=ALLBUT0999&response=csv"
    filename = os.path.join(OUTPUT_DIR, f"{target_date}_MI_INDEX_Sector.csv")
    
    _check_folder_and_create(filename) # 確保目錄存在
    
    print(f"  -> 嘗試抓取 {target_date}...")
    
    response_text = _fetch_twse_data(url)
    if response_text is None: 
        return None
    
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '指數' in df.columns:
        # 清理 '指數' 欄位中的空白，並移除空列
        df = df[df['指數'].astype(str).str.strip() != '']
        
        # 儲存資料
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"  ✅ {target_date} 資料儲存成功: {filename}")
        return df
        
    return None

# --- 批次處理與重試函數 (滿足使用者 2/3/4/5 點需求) ---

def batch_fetch_twse_mi_index(date_list: List[str]):
    """
    針對提供的日期清單，逐一抓取 TWSE MI_INDEX 資料，並在失敗時實作重試機制。
    Args:
        date_list: 包含 YYYYMMDD 格式日期的字串列表。
    """
    print("--- 開始批次抓取 TWSE MI_INDEX 資料 ---")
    
    for target_date in date_list:
        max_attempts = 4  # 首次嘗試 (1) + 3 次重試 = 最多 4 次機會
        
        for attempt in range(1, max_attempts + 1):
            
            # 執行抓取
            df = fetch_twse_mi_index_single(target_date)
            
            if df is not None:
                # 成功
                print(f"🌟 {target_date} 資料已完成。")
                break  # 成功，跳出重試迴圈
            
            # 失敗處理
            if attempt < max_attempts:
                # 遞增延遲時間: 第一次失敗延遲 1 小時, 第二次 2 小時, ...
                # (attempt - 1) 代表第幾次重試 (1, 2, 3...)
                delay_hours = attempt 
                
                # ⚠️ 實際生產環境請使用: delay_seconds = delay_hours * 3600
                # 測試環境用:
                delay_seconds = delay_hours * 5 

                print(f"🚨 {target_date} 抓取失敗 (第 {attempt} 次嘗試)。將在 {delay_seconds} 秒後重試 (下次等待 {delay_hours} 小時)...")
                time.sleep(delay_seconds)
            else:
                # 超過最大嘗試次數
                print(f"❌ {target_date} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此日期。")


# --- 執行範例 ---

if __name__ == "__main__":
    # 範例日期清單。請替換為您要抓取的實際日期。
    # 建議包含一些已知有資料的日期來測試成功案例。
    DATE_LIST_TO_FETCH = [
        "20251114", # 成功範例 1
        "20251115", # 成功範例 2
        "99991231"  # 失敗範例 (未來日期，將觸發重試)
    ]
    
    # 提醒：請先確保您的環境已安裝所需的函式庫：
    # pip install requests pandas

    batch_fetch_twse_mi_index(DATE_LIST_TO_FETCH)

# TEST_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_datas", "raw" , "2_MI_INDEX","test_output.csv" )
# print(TEST_OUTPUT_DIR)
# _check_folder_and_create(TEST_OUTPUT_DIR)
    