import pandas as pd
import requests
import os
from io import StringIO
from typing import Optional
#from requests.packages.urllib3.exceptions import InsecureRequestWarning

# 抑制發出 verify=False 相關的警告
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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
        csv_data_string = response.content.decode('cp950', errors='ignore')
        
        # 使用 StringIO 模擬檔案，讓 Pandas 讀取字串
        data_io = StringIO(csv_data_string)

        # 讀取 CSV。通常 TWSE 的 CSV 結構是：
        # 第一行: 查詢條件描述
        # 第二行: 真正的欄位名稱 (Header)
        # 第三行起: 實際資料
        # 因此我們指定 header=1 (從 0 開始計數，即第二行)
        df = pd.read_csv(data_io, header=1, encoding='cp950')
        
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
        df.to_csv(filename, index=False, encoding='cp950    ')
        print(f"💾 資料已成功儲存至 {filename}")
    except Exception as e:
        print(f"❌ 儲存檔案失敗: {e}")

# --- 新增函式：生成全年度日期清單 ---
def generate_full_year_dates(year: int) -> list[str]:
    """
    生成指定年份的所有日期清單 (從 1 月 1 日到 12 月 31 日)。
    
    Args:
        year (int): 要生成日期的年份 (例如 2024)。

    Returns:
        list[str]: 包含所有日期的列表，格式為 'YYYY/MM/DD'。
    """
    print(f"\n📅 正在生成 {year} 年的全年度日期清單...")
    try:
        # 設置起始日和結束日
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # 使用 Pandas date_range 產生所有日期，頻率為 'D' (Day)
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')

        # 將日期格式化為 'YYYY/MM/DD' 字串列表
        dates_list = all_dates.strftime('%Y/%m/%d').tolist()
        print(f"✅ 成功生成 {len(dates_list)} 筆日期。")
        return dates_list
    except Exception as e:
        print(f"❌ 生成日期清單時發生錯誤: {e}")
        return []


# --- 主程式執行區 ---

#----------------
if __name__ == '__main__':
    
    # 範例：抓取 2023 年的休市資料 (與您提供的 URL 參數一致)
    target_year = 2024 
    
    # 您可以修改這裡來抓取其他年份，例如 target_year = datetime.now().year
    
    holidays_df = fetch_twse_holidays(target_year)

    if holidays_df is not None and not holidays_df.empty:
        print("\n--- 抓取到的休市日期資料 (前 5 筆) ---")
        
        
        reversed_holidays = holidays_df['日期'].to_string(index=False)
        #print(holidays_df.head().to_string(index=False))
        print("--------讀取日期------------------------")
        print(holidays_df['日期'].to_string(index=False).strip()[:-3])
        
        holidays_list = []
        for holiday in holidays_df['日期']:
            month_holiday = holiday[:-3].split("月")[0].zfill(2)
            day_holiday = holiday[:-3].split("月")[1].replace("日","").strip().zfill(2)
            
        #    print(f"股票休市日期:{target_year}{month_holiday}{day_holiday}")
            holidays_list.append(f"{target_year}{month_holiday}{day_holiday}")
        
        print("---------------------------------------")
        # 儲存結果
        
        save_dataframe_to_csv(holidays_df, target_year)
    else:
        print("無法取得休市日期資料，請檢查網路連線或 TWSE 網址。")



#----------------

# ----------------------------------------------------
    # 新增：生成全年度日期清單 (年度為變數 target_year)
    # ----------------------------------------------------
    full_year_dates_list = generate_full_year_dates(target_year)
    # ----------------------------------------------------

    if holidays_df is not None and not holidays_df.empty:
        
        # 1. 提取 '日期' 欄位，並將格式從 YYY/MM/DD 轉換為 MM/DD
        # 例如 '113/01/01' 變成 '01/01'
        dates_md = holidays_df['日期'].astype(str).str.split('/').str[1:].str.join('/')
        
        # 2. 合併設定的年份 (target_year) 與 月/日 (MM/DD)
        # 例如 '2024' + '/' + '01/01' 變成 '2024/01/01'
        target_year_str = str(target_year)
        dates_gregorian = target_year_str + '/' + dates_md
        
        # 3. 將 MM/DD 格式轉換為 MMDD 數值字串格式 (例如 '01/01' 變成 '0101')
        dates_mmdd = dates_md.str.replace('/', '', regex=False)
        
        # 4. 輸出各種格式的資料
        print(f"\n--- TWSE 休市日期與全年度日期 ({target_year} 年) ---")
        
        # 輸出 YYYY/MM/DD 格式
        print("\n[A] 休市日 YYYY/MM/DD (完整西元日期):")
        print(dates_gregorian.to_string(index=False))
        
        # 輸出 MMDD 格式 (回應使用者要求)
        print("\n[B] 休市日 MMDD (月日數值字串):")
        print(dates_mmdd.to_string(index=False))
        
        # 5. 取得休市日的總數量
        total_holidays = len(holidays_df)
        print(f"\n[C] 休市日的總數量: {total_holidays} 天")

        # 6. 輸出全年度日期清單
        print("\n[D] 全年度日期清單 (前 10 筆):")
        if full_year_dates_list:
            # 只顯示前 10 筆，避免輸出過長
            for i, date_str in enumerate(full_year_dates_list[:10]):
                print(date_str)
            if len(full_year_dates_list) > 10:
                 print(f"... 還有 {len(full_year_dates_list) - 10} 筆日期 (總數: {len(full_year_dates_list)} 筆)")
        else:
             print("全年度日期清單為空。")
        
        print("\n---------------------------------------")

        # 儲存結果 (儲存時仍包含原始完整日期資料，以利檔案使用)
        save_dataframe_to_csv(holidays_df, target_year)
    else:
        # 僅有抓取TWSE休市資料失敗時，才顯示錯誤訊息
        print("無法取得休市日期資料，請檢查網路連線或 TWSE 網址。")
        
        # 即使抓取失敗，也要展示全年度日期生成功能
        if full_year_dates_list:
            print(f"\n--- 僅展示全年度日期清單 ({target_year} 年) ---")
            print("[D] 全年度日期清單 (前 10 筆):")
            for i, date_str in enumerate(full_year_dates_list[:10]):
                print(date_str)
            if len(full_year_dates_list) > 10:
                 print(f"... 還有 {len(full_year_dates_list) - 10} 筆日期 (總數: {len(full_year_dates_list)} 筆)")


print(holidays_list)