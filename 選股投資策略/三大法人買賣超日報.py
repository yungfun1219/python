import requests
import pandas as pd
from io import StringIO
import urllib3
import re
import os # 導入 os 模組用於處理檔案路徑

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_twse_t86_data(date: str) -> pd.DataFrame or None:
    """
    從臺灣證券交易所 (TWSE) 抓取指定日期的 T86 報告 (自營商買賣超彙總表)，
    並將結果存為 CSV 檔案，檔名為 [日期]_T86.csv。

    Args:
        date: 欲查詢的日期，格式為 YYYYMMDD (例如: "20251007")。

    Returns:
        包含 T86 資料的 Pandas DataFrame；若抓取失敗則返回 None。
    """
    # 驗證日期格式
    if not re.fullmatch(r'\d{8}', date):
        print(f"錯誤：日期格式 {date} 不正確，請使用 YYYYMMDD 格式。")
        return None
        
    # 組合完整的 URL
    base_url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    url = f"{base_url}?date={date}&selectType=ALLBUT0999&response=csv"
    
    # 設定檔案名稱
    filename = f"{date}_T86.csv"
    
    print(f"嘗試抓取日期：{date} 的資料...")

    try:
        # 1. 使用 requests 獲取網頁內容
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 解決 SSL 錯誤：加入 verify=False 忽略憑證驗證
        response = requests.get(url, headers=headers, verify=False)
        
        # 檢查 HTTP 狀態碼 (若無此日資料，通常會回傳 404/400 錯誤)
        response.raise_for_status() 

        # 2. 處理資料編碼
        response.encoding = 'Big5'

        # 3. 轉換成檔案狀的物件並讀取成 DataFrame
        csv_data = StringIO(response.text)
        df = pd.read_csv(
            csv_data, 
            header=1,                  
            skipinitialspace=True,     
            on_bad_lines='skip',       
            encoding='Big5'            
        )
        
        # 4. 清理資料
        df = df[~df['證券代號'].astype(str).str.contains('合計|總計|備註', na=False)]
        df = df.dropna(how='all') 
        
        # 5. 【新增步驟】將 DataFrame 儲存為 CSV 檔案
        # index=False 表示不將 DataFrame 的行索引寫入 CSV 檔案
        # encoding='utf-8-sig' 可確保中文在 Excel 中開啟時不會亂碼
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 日期 {date} 資料抓取成功，已儲存為檔案：{filename}")
        return df
    
    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP 錯誤：{errh} (可能是該日無交易資料)")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

# --- 範例使用 ---

# 設定您想要抓取的日期
target_date_1 = "20251007"
df_data_1 = fetch_twse_t86_data(target_date_1)

# 範例 2: 抓取另一個日期 (例如 2024年6月28日)
# target_date_2 = "20240628" 
# df_data_2 = fetch_twse_t86_data(target_date_2)