import requests
import pandas as pd
from io import StringIO
import urllib3
import re
from datetime import date
from typing import Optional

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 配置 (Configuration) ---
LOG_FILENAME = "last_fetch_date.log" # 定義日誌檔案名稱

# --- 輔助函式 (Helper Functions) ---
def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """
    共用的輔助函式，用於處理 TWSE 的 Big5 編碼和 Pandas 讀取邏輯。
    
    Args:
        response_text: HTTP 請求回傳的文字內容 (Big5 編碼)。
        header_row: CSV 檔案中資料表頭所在的行數 (0-indexed)。

    Returns:
        Optional[pd.DataFrame]: 處理後的 DataFrame。
    """
    try:
        csv_data = StringIO(response_text)
        # 嘗試自動尋找標題列
        preview = response_text.splitlines()
        for i, line in enumerate(preview[:10]):
            if '證券代號' in line or '日期' in line or '類股名稱' in line:
                header_row = i
                break

        df = pd.read_csv(
            StringIO(response_text),
            header=header_row,
            encoding='Big5',
            skipinitialspace=True,
            on_bad_lines='skip'
        )
        df.columns = df.columns.str.strip()
        df = df.dropna(how='all')
        return df
    except Exception as e:
        print(f"讀取 CSV 錯誤: {e}")
        return None

def _fetch_twse_data(url: str) -> Optional[str]:
    """
    共用的輔助函式，用於發送 HTTP 請求並檢查狀態。
    
    Args:
        url: 完整的 TWSE 資料 URL。

    Returns:
        Optional[str]: 成功獲取後，以 Big5 解碼的文字內容。
    """
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
        
    return None


test_date = "20251009"
url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={test_date}&selectType=ALLBUT0999&response=csv"
txt = _fetch_twse_data(url)
print(txt[:300])