import os
import re
import time
import requests
import pandas as pd
from typing import Optional, List
from io import StringIO
import sys
from datetime import datetime, timedelta
import pathlib     # as pathlib

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 設定與路徑 ---
# ⚠️ 請確保 'datas/raw/3_BWIBBU_d' 路徑存在或可被建立
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "3_BWIBBU_d")
BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"

Now_time_year = datetime.now().strftime("%Y")  #取得目前系統時間的「年」
CSV_FILE_PATH = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-{Now_time_year}.csv"

def get_date_list_based_on_time(file_path: str) -> Optional[List[str]]:
    """
    1. 讀取 CSV 檔案內的日期 (假定為交易日清單)。
    2. 根據當前時間 (21:00 前/後) 確定截止日期 (昨天/今天)。
    3. 輸出從檔案第一個日期到截止日期的日期清單。
    """
    
    # 1. 讀取 CSV 檔案
    try:
        # 讀取 CSV，假設日期在第一欄
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 嘗試找出包含日期的欄位 (假設是第一欄)
        date_column = df.columns[0]
        
        # 過濾空值並轉換為已排序的字串列表 (格式為 YYYYMMDD)
        all_dates_list = df[date_column].astype(str).str.strip().tolist()
        all_dates_list = sorted(list(set(all_dates_list)))

        if not all_dates_list:
            print(f"錯誤: 檔案 {file_path} 中找不到任何日期數據。")
            return None

    except FileNotFoundError:
        print(f"錯誤: 找不到檔案 {file_path}，請確認路徑或先運行模擬初始化。")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理檔案 {file_path} 時發生錯誤: {e}")
        return None

    # 2. 判斷現在的時間來決定截止日期
    now = datetime.now()
    current_time = now.time()
    
    # 定義 21:00 (晚上 9 點) 的截止時間
    cutoff_time = datetime.strptime("21:00:00", "%H:%M:%S").time()
    
    if current_time >= cutoff_time:
        # 21點以後 (含 21:00:00): 截止日為今天
        end_date = now.date()
        print(f"【時間判斷】當前時間 ({now.strftime('%H:%M:%S')}) 晚於 21:00，截止日為今天 ({end_date.strftime('%Y/%m/%d')})。")
    else:
        # 21點以前: 截止日為昨天
        end_date = (now - timedelta(days=1)).date()
        print(f"【時間判斷】當前時間 ({now.strftime('%H:%M:%S')}) 早於 21:00，截止日為昨天 ({end_date.strftime('%Y/%m/%d')})。")

    # 3. 確定日期範圍
    start_date_str = all_dates_list[0]
    end_date_str = end_date.strftime("%Y%m%d")
    
    # 4. 篩選 CSV 內的日期清單
    # 只保留介於 [起始日期, 截止日期] 之間的所有日期
    filtered_dates = [
        date_str for date_str in all_dates_list 
        if start_date_str <= date_str <= end_date_str
    ]

    if not filtered_dates:
        print(f"警告: 在範圍 [{start_date_str} - {end_date_str}] 內找不到任何日期。")
        return []
        
    print(f"\n--- 最終日期清單 (共 {len(filtered_dates)} 天) ---")
    print(f"起始日期: {filtered_dates[0]}")
    print(f"截止日期: {filtered_dates[-1]}")
    
    return filtered_dates
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
        time.sleep(5)
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
        # header_row=1: BWIBBU_d 報表實際的表頭在索引 1 (0-based)
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

def fetch_twse_BWIBBU_d_single(target_date: str) -> Optional[pd.DataFrame]:
    """
    抓取指定日期的 BWIBBU_d 報告。

    Args:
        target_date: 欲抓取的日期，格式為 YYYYMMDD。

    Returns:
        成功時返回 DataFrame，失敗時返回 None。
    """
    if not re.fullmatch(r'\d{8}', target_date): 
        print(f"日期格式錯誤: {target_date}")
        return None
        
    url = f"{BASE_URL}?date={target_date}&type=ALLBUT0999&response=csv"
    filename = os.path.join(OUTPUT_DIR, f"{target_date}_BWIBBU_d_IndexReturn.csv")
    
    _check_folder_and_create(filename) # 確保目錄存在
    
    print(f"  -> 嘗試抓取 {target_date}...")
    
    response_text = _fetch_twse_data(url)
    if response_text is None: 
        # 資料抓取失敗，_fetch_twse_data 已處理錯誤訊息
        return None
    
    df = _read_twse_csv(response_text, header_row=1)

    # VVVVVVVVVVVVVVVVVVVVVVVVVV
    # 關鍵修正：在嘗試 to_csv 之前，檢查 df 是否為 None
    if df is not None:
        # 執行數據清理
        if '證券代號' in df.columns:
            # 清理 '證券代號' 欄位中的空白，並移除空列
            df = df[df['證券代號'].astype(str).str.strip() != '']

        # 儲存資料
        # 這一行就是之前發生錯誤的地方，現在有 df is not None 的保護
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"  ✅ {target_date} 資料儲存成功: {filename}")
        except Exception as e:
            print(f"❌ {target_date} 資料儲存失敗: {e}")
            return None # 儲存失敗也返回 None
            
        return df
    else:
        # 如果 df 是 None，表示解析或數據本身有問題，發出警告
        print(f"  ⚠️ {target_date} 資料抓取成功但解析後為空，跳過儲存。")
        return None

# --- 批次處理與重試函數 (滿足使用者 2/3/4/5 點需求) ---

def batch_fetch_twse_BWIBBU_d(date_list: List[str]):
    """
    針對提供的日期清單，逐一抓取 TWSE BWIBBU_d 資料，並在失敗時實作重試機制。
    Args:
        date_list: 包含 YYYYMMDD 格式日期的字串列表。
    """
    print("--- 開始批次抓取 TWSE BWIBBU_d 資料 ---")
    
    for target_date in date_list:
        target_date = target_date.replace("/", "")
        max_attempts = 1  # 首次嘗試 (1) + 3 次重試 = 最多 4 次機會
        
        for attempt in range(1, max_attempts + 1):
            
            # 執行抓取
            df = fetch_twse_BWIBBU_d_single(target_date)

            
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
        print("等待 2 秒後，準備處理下一個日期...")
        time.sleep(2)

# --- 執行範例 ---

if __name__ == "__main__":
    # 範例日期清單。請替換為您要抓取的實際日期。
    # 建議包含一些已知有資料的日期來測試成功案例。
    
    # 2. 執行日期清單生成
    final_date_list = get_date_list_based_on_time(CSV_FILE_PATH)
    # final_date_list = ["20210101"]
    # DATE_LIST_TO_FETCH = [
    #     "20251114", # 成功範例 1
    #     "20251115", # 成功範例 2
    #     "99991231"  # 失敗範例 (未來日期，將觸發重試)
    # ]
    
    # 提醒：請先確保您的環境已安裝所需的函式庫：
    # pip install requests pandas

    print(final_date_list)

    batch_fetch_twse_BWIBBU_d(final_date_list)

# TEST_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_datas", "raw" , "3_BWIBBU_d","test_output.csv" )
# print(TEST_OUTPUT_DIR)
# _check_folder_and_create(TEST_OUTPUT_DIR)
    