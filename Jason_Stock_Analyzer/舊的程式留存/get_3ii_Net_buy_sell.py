import requests
import pandas as pd
from io import StringIO
import urllib3
import re
import os # 處理檔案路徑
from datetime import datetime, date # 處理日期
from pathlib import Path

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_twse_t86_data(date: str) -> pd.DataFrame or None:
    """
    從臺灣證券交易所 (TWSE) 抓取指定日期的 T86 報告 (自營商買賣超彙總表)，
    並將結果存到指定的目錄結構中：Datas/raw/3ii_Net_buy_sell/[日期]_3ii_Net_buy_sell.csv。

    Args:
        date: 欲查詢的日期，格式為 YYYYMMDD (例如: "20251009")。

    Returns:
        包含 T86 資料的 Pandas DataFrame；若抓取失敗則返回 None。
    """
    # 驗證日期格式
    if not re.fullmatch(r'\d{8}', date):
        print(f"錯誤：日期格式 {date} 不正確，請使用 YYYYMMDD 格式。")
        return None
        
    # 1. 定義目標輸出目錄和檔案名稱
    
    # 相對路徑：從程式執行的目錄開始計算
    output_dir_relative = r"Datas\raw\3ii_Net_buy_sell"
    
    # 取得目前執行檔案的 Path 物件
    current_file = Path(__file__)
    current_dir = current_file.parent.resolve()

    # 檔案名稱：[今天日期]_3ii_Net_buy_sell.csv
    filename = f"{date}_3ii_Net_buy_sell.csv"
    
    # 完整的檔案儲存路徑

    full_filepath = current_dir/output_dir_relative/filename
    print(f"嘗試抓取日期：{date} 的資料...")

    try:
        # 2. 建立目錄結構
        # exist_ok=True 允許目錄已經存在，不會拋出錯誤
        os.makedirs(output_dir_relative, exist_ok=True)
        print(f"-> 輸出目錄已確認/建立：{output_dir_relative}")
        
        # 3. 組合完整的 URL
        base_url = "https://www.twse.com.tw/rwd/zh/fund/T86"
        url = f"{base_url}?date={date}&selectType=ALLBUT0999&response=csv"

        # 4. 使用 requests 獲取網頁內容
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status() 

        # 5. 處理資料編碼並讀取成 DataFrame
        response.encoding = 'Big5'
        csv_data = StringIO(response.text)
        df = pd.read_csv(
            csv_data, 
            header=1,                
            skipinitialspace=True,   
            on_bad_lines='skip',     
            encoding='Big5'          
        )
        
        # 6. 清理資料
        df = df[~df['證券代號'].astype(str).str.contains('合計|總計|備註', na=False)]
        df = df.dropna(how='all') 
        
        # 7. 將 DataFrame 儲存為 CSV 檔案 (使用完整路徑)
        df.to_csv(full_filepath, index=False, encoding='utf-8-sig')
        
        print(f"✅ 日期 {date} 資料抓取成功，已儲存於：{full_filepath}")
        return df
    
    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP 錯誤：{errh} (可能是該日無交易資料)")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

# --- 範例使用 ---

# 1. 取得今天的日期並格式化為 YYYYMMDD
today = date.today()
target_date = today.strftime("%Y%m%d")

#target_date = "20251008"  # 測試用特定日期
# 2. 抓取今天的資料
print("--- 開始讀取【三大法人買賣超日報】資料 ---")
df_data = fetch_twse_t86_data(target_date)

#if df_data is not None:
#    print("\n[抓取數據預覽 (前 5 筆)]:")
#    print(df_data.head().to_markdown(index=False))
#
#print("--- 執行結束 ---")
