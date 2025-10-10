import requests
import pandas as pd
from io import StringIO
import urllib3
import re
from datetime import date
from typing import Optional

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_twse_mi_index20(target_date: str) -> Optional[pd.DataFrame]:
    """
    從臺灣證券交易所 (TWSE) 抓取指定日期的 MI_INDEX20 報告
    (收盤指數及成交量值資訊)，並將結果存為 CSV 檔案。

    Args:
        target_date (str): 欲查詢的日期，格式為 YYYYMMDD (例如: "20240628")。

    Returns:
        Optional[pd.DataFrame]: 包含 MI_INDEX20 資料的 Pandas DataFrame；若抓取失敗則返回 None。
    """
    # 驗證日期格式
    if not re.fullmatch(r'\d{8}', target_date):
        print(f"錯誤：日期格式 {target_date} 不正確，請使用 YYYYMMDD 格式。")
        return None
        
    # 組合完整的 URL
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20"
    # date 參數為模組化設計
    url = f"{base_url}?date={target_date}&response=csv"
    
    # 設定檔案名稱：使用 [日期]_MI_INDEX20_Analysis.csv 格式
    filename = f"{target_date}_MI_INDEX20_Analysis.csv"
    
    print(f"嘗試抓取日期：{target_date} 的大盤統計資料...")

    try:
        # 1. 使用 requests 獲取網頁內容
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 由於 TWSE 伺服器配置，使用 verify=False 忽略憑證驗證
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status() # 檢查 HTTP 狀態碼
        
        # 2. 處理資料編碼：TWSE CSV 通常為 Big5
        response.encoding = 'Big5'

        # 3. 轉換成檔案狀的物件並讀取成 DataFrame
        csv_data = StringIO(response.text)
        
        # 讀取 CSV 時，通常需要跳過 TWSE 資料前的標題行 (head=1)
        df = pd.read_csv(
            csv_data, 
            header=1,                # 資料從第二行開始 (索引 1)
            skipinitialspace=True,   
            on_bad_lines='skip',     
            encoding='Big5'          
        )
        
        # 4. 清理資料：移除不必要的行
        # MI_INDEX20 資料最上方可能還有額外的說明行，我們再次清理。
        # 確保 '指數名稱' 欄位存在且非空
        if '指數名稱' in df.columns:
            # 移除所有欄位皆為空的行，並清理資料尾部可能出現的彙總或備註行
            df = df.dropna(how='all') 
            df = df[df['指數名稱'].astype(str).str.strip() != '']
        
        # 5. 將 DataFrame 儲存為 CSV 檔案
        # index=False：不將 DataFrame 的行索引寫入 CSV
        # encoding='utf-8-sig'：確保中文在 Excel 中開啟時不會亂碼
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 日期 {target_date} 資料抓取成功，已儲存為檔案：{filename}")
        return df
    
    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP 錯誤：{errh} (可能是該日無交易資料或日期太久遠)")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

# --- 範例使用 (將日期設定為模組化參數) ---

# 範例 1：使用一個近期的日期
# 您可以直接在這裡輸入您想抓取的日期 (YYYYMMDD)
target_date_1 = "20240628" 
df_data_1 = fetch_twse_mi_index20(target_date_1)

if df_data_1 is not None:
    print(f"\n[日期 {target_date_1} 數據預覽 (前 5 筆)]:")
    print(df_data_1.head().to_markdown(index=False))

print("\n--- 程式執行結束 ---")
