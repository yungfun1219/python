import pandas as pd
import requests
import re
import os
import time
from typing import Optional, List

# --- 模擬您程式中使用的輔助函數 ---

def check_folder_and_create(filepath: str):
    """
    確保檔案所在的目錄存在。
    """
    dir_path = os.path.dirname(filepath)
    os.makedirs(dir_path, exist_ok=True)
    
def get_recent_trading_dates(num_days: int = 5) -> List[str]:
    """
    獲取最近的幾個可能的交易日期 (YYYYMMDD 格式)
    這只是生成日期，並不保證是交易日，但用於測試很方便。
    """
    end_date = pd.to_datetime('today').normalize()
    # 嘗試從今天開始回溯，包含週末，這樣確保我們涵蓋了至少 5 個可能的日期
    date_range = pd.date_range(end=end_date, periods=num_days * 2, freq='D')
    
    # 過濾週末 (星期六=5, 星期日=6)
    trading_dates = date_range[date_range.dayofweek < 5].strftime('%Y%m%d').tolist()
    
    # 返回最近的 num_days 個日期
    return trading_dates[-num_days:]

# --- 核心抓取函數 (使用 requests 和 pandas 實作) ---

def fetch_twse_twt44u(target_date: str) -> Optional[pd.DataFrame]:
    """
    (10/10) 抓取指定日期的 TWT44U 報告 (投信買賣超彙總表)。
    
    Args:
        target_date: YYYYMMDD 格式的目標日期字串。
        
    Returns:
        成功則返回 DataFrame，失敗則返回 None。
    """
    
    # 1. 驗證日期格式
    if not re.fullmatch(r'\d{8}', target_date): 
        print(f"❌ 日期格式錯誤: {target_date}。必須是 YYYYMMDD 格式。")
        return None
    
    print(f"\n嘗試抓取 (10/10) TWT44U (投信買賣超) 資料，目標日期: {target_date}...")
    
    base_url = "https://www.twse.com.tw/rwd/zh/fund/TWT44U"
    url = f"{base_url}?date={target_date}&response=csv"
    
    # 2. 檔案路徑設定
    # 這裡假設腳本位於一個合理的目錄結構中
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "10_TWT44U")
    filename = os.path.join(OUTPUT_DIR, f"{target_date}_TWT44U_InvestmentTrust.csv")
    
    check_folder_and_create(filename)

    # 3. 網絡請求和延遲 (關鍵改進點)
    try:
        # 加上延遲，防止請求過快被網站拒絕或封鎖
        print("⏸️ 等待 3 秒後發送請求...")
        time.sleep(3) 
        
        # 替代您原有的 _fetch_twse_data 邏輯
        # TWSE 的 CSV 網頁可能需要特定的 headers 來偽裝成瀏覽器
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        
        # 檢查 HTTP 狀態碼
        if response.status_code != 200:
            print(f"❌ HTTP 請求失敗，狀態碼: {response.status_code}")
            return None

        # TWSE 資料通常是 BIG5 編碼，我們讀取原始內容並轉碼
        response_text = response.content.decode('utf-8') 
        
    except requests.exceptions.RequestException as e:
        print(f"❌ 網路請求發生錯誤: {e}")
        return None

    # 4. 資料解析 (關鍵改進點)
    # 替代您原有的 _read_twse_csv 邏輯
    try:
        # TWSE 的 CSV 格式通常第一行是標題，第二行才是欄位名稱，所以我們使用 header=1 (索引從 0 開始)
        from io import StringIO
        df = pd.read_csv(StringIO(response_text), header=1)
    except Exception as e:
        print(f"❌ 資料解析失敗: {e}")
        return None

    # 5. 數據有效性檢查與儲存
    if df is not None and '證券代號' in df.columns:
        # 檢查是否有"本日無交易資料"這類字樣的錯誤訊息
        if df.iloc[0, 0] == '本日無交易資料':
             print(f"⚠️ TWSE 回覆: 本日無交易資料。請確認 {target_date} 是否為交易日。")
             return None
        
        # 數據清洗：移除證券代號為空的列
        original_rows = len(df)
        df = df[df['證券代號'].astype(str).str.strip() != '']
        cleaned_rows = len(df)
        
        if cleaned_rows == 0:
            print(f"⚠️ {target_date} 數據清洗後為空。可能為無效數據或網站格式變動。")
            return None
        
        # 儲存檔案
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (10/10) {filename} 儲存成功，共 {cleaned_rows} 筆資料。")
        return df
    else:
        print("❌ 資料解析後不包含 '證券代號' 欄位，或 DataFrame 結構不正確。")
        print(f"偵測到的欄位: {df.columns.tolist() if df is not None else 'N/A'}")
        return None

if __name__ == '__main__':
    # 獲取最近 3 個可能的交易日進行測試
    test_dates = get_recent_trading_dates(num_days=3)
    
    print("--- 開始 TWSE TWT44U 資料抓取測試 ---")
    
    for date in test_dates:
        result_df = fetch_twse_twt44u(date)
        
        if result_df is not None:
            print(f"成功抓取並儲存 {date} 的資料。")
        else:
            print(f"未能抓取 {date} 的資料。")
            
    print("\n--- 測試結束 ---")
    
    # 測試一個無效的日期 (例如週末或未來的日期)
    fetch_twse_twt44u("20251012") # 假設 10/12 是週六或週日
    
    # 測試一個錯誤格式的日期
    fetch_twse_twt44u("2025-10-10")
