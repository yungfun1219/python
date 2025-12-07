import os
import re
import time
import requests
import pandas as pd
from typing import Optional, List
from io import StringIO
from datetime import datetime, timedelta
import pathlib     # as pathlib

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 設定與路徑 ---
# ⚠️ 請確保 'datas/raw/7_FMTQIK' 路徑存在或可被建立
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "7_FMTQIK")
BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"

# 假設交易日清單檔案為 trading_day_2021-2025.csv
CSV_FILE_PATH = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-2025.csv"

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
    
    # 檢查當前時間（2025/11/22 21:03:14 CST）
    if current_time >= cutoff_time:
        # 21點以後 (含 21:00:00): 截止日為今天 (11/22)
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
# --- 輔助函數 ---
# 檢查並建立所需的【資料夾】
def _check_folder_and_create(filepath: str):
    # 使用 pathlib 確保跨平台兼容性
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
            print("⚠️ TWSE 網站返回錯誤訊息，該日可能無資料。")
            return None
        
        return response.text
        
    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP 錯誤：{errh} (該日可能無交易資料)")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """將 TWSE 返回的文本解析為 Pandas DataFrame。"""
    try:
        data = StringIO(response_text)
        # 報表實際的表頭在索引 1 (0-based)
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
                print("⚠️ 解析 CSV 後 DataFrame 為空，可能無有效資料。")
                return None
            
            # FMTQIK 報表通常以「券商」作為第一欄位，移除空白行
            if '券商' in df.columns:
                 df = df[df['券商'].astype(str).str.strip() != '']
                
            return df
        return None

    except Exception as e:
        print(f"❌ 解析 CSV 數據時發生錯誤: {e}")
        return None


# --- 核心單日抓取函數 ---

def fetch_twse_fmtqik_single(target_date: str) -> Optional[pd.DataFrame]:
    """
    抓取指定日期的 FMTQIK 報告 (券商成交量值總表)。

    Args:
        target_date: 欲抓取的日期，格式為 YYYYMMDD。

    Returns:
        成功時返回 DataFrame，失敗時返回 None。
    """
    if not re.fullmatch(r'\d{8}', target_date): 
        print(f"日期格式錯誤: {target_date}")
        return None
        
    url = f"{BASE_URL}?date={target_date}&response=csv"
    # 使用 os.path.join 確保跨平台兼容性
    filename = os.path.join(OUTPUT_DIR, f"{target_date}_FMTQIK_BrokerVolume.csv") 
    
    _check_folder_and_create(filename) # 確保目錄存在
    
    print(f"  -> 嘗試抓取 {target_date}...")
    
    response_text = _fetch_twse_data(url)
    if response_text is None: 
        return None
    
    # 假設 FMTQIK 的表頭在索引 1
    df = _read_twse_csv(response_text, header_row=1) 

    if df is not None:
        
        # 儲存資料
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"  ✅ {target_date} 資料儲存成功: {filename}")
        except Exception as e:
            print(f"❌ {target_date} 資料儲存失敗: {e}")
            return None # 儲存失敗也返回 None
            
        return df
    else:
        print(f"  ⚠️ {target_date} 資料抓取成功但解析後為空，跳過儲存。")
        return None

# --- 批次處理與重試函數 (檔案存在不等待 2 秒) ---

def batch_fetch_twse_fmtqik(date_list: List[str]):
    """
    針對提供的日期清單，逐一抓取 TWSE FMTQIK 資料，並在失敗時實作重試機制。
    """
    print("--- 開始批次抓取 TWSE FMTQIK 資料 ---")
    
    for target_date in date_list:
        target_date = target_date.replace("/", "")
        max_attempts = 4
        
        filename = os.path.join(OUTPUT_DIR, f"{target_date}_FMTQIK_BrokerVolume.csv")
        _check_folder_and_create(filename) # 確保目錄存在

        # 關鍵修正：步驟 1. 檢查檔案是否已存在，存在則跳過延遲
        if os.path.exists(filename):
            print(f"  ℹ️ {target_date} 資料已存在 ({filename})，跳過抓取。")
            continue  # 立即跳到下一個日期，不執行延遲
        
        # 步驟 2. 檔案不存在，開始執行抓取和重試
        is_successful = False
        for attempt in range(1, max_attempts + 1):
            
            # 執行抓取
            df = fetch_twse_fmtqik_single(target_date)
            
            if df is not None:
                # 成功
                print(f"🌟 {target_date} 資料已完成。")
                is_successful = True
                break  # 成功，跳出重試迴圈
            
            # 失敗處理
            if attempt < max_attempts:
                delay_hours = attempt 
                
                # 測試環境用:
                delay_seconds = delay_hours * 5 

                print(f"🚨 {target_date} 抓取失敗 (第 {attempt} 次嘗試)。將在 {delay_seconds} 秒後重試 (下次等待 {delay_hours} 小時)...")
                time.sleep(delay_seconds)
            else:
                # 超過最大嘗試次數
                print(f"❌ {target_date} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此日期。")
        
        # 步驟 3. 只有在執行了網路抓取或重試之後，才需要等待 2 秒
        if is_successful or attempt == max_attempts:
            print("等待 2 秒後，準備處理下一個日期...")
            time.sleep(2)


# --- 執行範例 ---

if __name__ == "__main__":
    
    # 2. 執行日期清單生成
    final_date_list = get_date_list_based_on_time(CSV_FILE_PATH)
    
    if final_date_list:
        print("--- 開始抓取/檢查檔案 ---")
        batch_fetch_twse_fmtqik(final_date_list)
    else:
        print("沒有可供處理的日期清單，程式結束。")