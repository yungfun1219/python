import pandas as pd
import requests
import os
from io import StringIO
from typing import Optional
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# 抑制發出 verify=False 相關的警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def fetch_twse_holidays(year: int) -> Optional[pd.DataFrame]:
    """
    從台灣證券交易所網站抓取指定年份的休市日期資料。
    
    TWSE 的 CSV 格式通常在開頭有幾行描述，需要特別處理。
    
    Args:
        year (int): 要抓取的年份 (例如 2024)。

    Returns:
        Optional[pd.DataFrame]: 包含休市日期的 DataFrame，如果抓取失敗則回傳 None。
    """
    # TWSE 網址結構：我們使用該年份的第一天作為查詢基準點
    twse_url = (
        f"https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
        f"?date={year}0101&response=csv"
    )
    
    print(f"📡 嘗試抓取 {year} 年 TWSE 休市資料...")
    print(f"URL: {twse_url}")
    print("⚠️ 注意：已禁用 SSL 憑證驗證 (verify=False) 以解決連線問題。")

    try:
        # 使用 requests 獲取內容，並指定編碼為 'big5' 或 'cp950' 
        # 以確保繁體中文不會亂碼 (TWSE 舊系統常用此編碼)
        # *** 解決 SSLCertVerificationError 錯誤：加入 verify=False ***
        response = requests.get(twse_url, verify=False)
        
        # 檢查 HTTP 狀態碼
        if response.status_code != 200:
            print(f"❌ 網路請求失敗，狀態碼: {response.status_code}")
            return None
        
        # 將內容從 big5 解碼為字串，以便 Pandas 讀取
        # 由於 TWSE CSV 的格式比較特殊，開頭有額外的標題行，
        # 我們通常需要跳過第一行或指定表頭位置。
        csv_data_string = response.content.decode('big5')
        
        # 使用 StringIO 模擬檔案，讓 Pandas 讀取字串
        data_io = StringIO(csv_data_string)

        # 讀取 CSV。通常 TWSE 的 CSV 結構是：
        # 第一行: 查詢條件描述
        # 第二行: 真正的欄位名稱 (Header)
        # 第三行起: 實際資料
        # 因此我們指定 header=1 (從 0 開始計數，即第二行)
        df = pd.read_csv(data_io, header=1, encoding='big5')
        
        # 數據清理步驟：
        # 1. 移除最後幾行可能出現的空白或註釋行 (通常會以 NaN 呈現)
        df.dropna(how='all', inplace=True)
        
        # 2. 重新命名欄位為中文 (如果 TWSE 提供的欄位名稱不是標準中文)
        # 根據實際資料結構，假設欄位為 '日期' 和 '說明'
        if len(df.columns) >= 4:
            df.columns = ['日期', '名稱', '說明', '備註']
        
        # 3. 確保日期欄位為標準格式 (如果需要)
        # df['日期'] = pd.to_datetime(df['日期'], errors='coerce')

        print(f"✅ 成功抓取 {len(df)} 筆休市資料。")
        return df

    except Exception as e:
        print(f"❌ 抓取或處理資料時發生錯誤: {e}")
        return None

def save_dataframe_to_csv(df: pd.DataFrame, year: int):
    """將 DataFrame 儲存為本地 CSV 檔案。"""
    filename = f"twse_holidays_{year}.csv"
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"💾 資料已成功儲存至 {filename}")
    except Exception as e:
        print(f"❌ 儲存檔案失敗: {e}")


# --- 主程式執行區 ---
if __name__ == '__main__':
    
    # 範例：抓取 2023 年的休市資料 (與您提供的 URL 參數一致)
    target_year = 2024 
    
    # 您可以修改這裡來抓取其他年份，例如 target_year = datetime.now().year
    
    holidays_df = fetch_twse_holidays(target_year)

    if holidays_df is not None and not holidays_df.empty:
        print("\n--- 抓取到的休市日期資料 (前 5 筆) ---")
        print(holidays_df.head().to_string(index=False))
        print("---------------------------------------")
        
        # 儲存結果
        save_dataframe_to_csv(holidays_df, target_year)
    else:
        print("無法取得休市日期資料，請檢查網路連線或 TWSE 網址。")
