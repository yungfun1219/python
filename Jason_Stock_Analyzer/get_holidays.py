import pandas as pd
import requests
import os
from io import StringIO
from typing import Optional
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
from datetime import date # 引入 date 以便使用

# 抑制發出 verify=False 相關的警告
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 函式：生成全年度週末日期清單 ---
def generate_full_year_weekend_dates(year: int) -> list[str]:
    """
    生成指定年份的所有週末日期清單 (星期六和星期日)。
    Args:
        year (int): 要生成日期的年份 (例如 2024)。
    Returns:
        list[str]: 包含所有週末日期的列表，格式為 'YYYY/MM/DD'。
    """
    print(f"\n📅 正在生成 {year} 年的全年度週末日期清單...")
    try:
        # 設置起始日和結束日
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # 使用 Pandas date_range 產生所有日期，頻率設為 'D' (Day)
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 過濾日期：保留星期六 (5) 和星期日 (6)。 (星期一=0, 星期日=6)
        all_dates = all_dates[all_dates.dayofweek.isin([5, 6])]

        # 將日期格式化為 'YYYY/MM/DD' 字串列表
        dates_list = all_dates.strftime('%Y/%m/%d').tolist()
        print(f"✅ 成功生成 {len(dates_list)} 筆日期 (僅包含週末)。")
        return dates_list
    except Exception as e:
        print(f"❌ 生成日期清單時發生錯誤: {e}")
        return []
# --- 結束函式 ---

# 新增：儲存日期清單到 CSV 檔案的函式
def save_dates_to_csv(dates_list: list[str], year: int, filename: str = 'weekend_dates.csv'):
    """
    將日期清單儲存為 CSV 檔案。
    
    Args:
        dates_list (list[str]): 要儲存的日期清單。
        year (int): 生成日期的年份 (用於檔案命名或日誌)。
        filename (str): 儲存的檔案名稱 (預設為 'weekend_dates.csv')。
    """
    if not dates_list:
        print("【警告】日期清單為空，不執行儲存操作。")
        return

    try:
        # 建立 DataFrame
        # 這裡假設您希望 CSV 檔案只有一欄，欄位名稱為 'Date'
        df = pd.DataFrame(dates_list, columns=['Date'])
        
        # 定義檔案路徑，儲存在當前目錄
        file_path = os.path.join(os.getcwd(), f"{year}_{filename}")
        
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "twse_holidays" , f"{year}_{filename}")
        # 儲存為 CSV 檔案
        # index=False 表示不寫入行索引
        # encoding='utf-8-sig' 可確保中文或特殊字元在 Excel 中正確顯示
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"\n💾 成功將 {len(dates_list)} 筆週末日期儲存到 CSV 檔案。")
        print(f"檔案路徑: {file_path}")
        
    except Exception as e:
        print(f"【錯誤】儲存 CSV 檔案時發生錯誤: {e}")


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
    #print("⚠️ 注意：已禁用 SSL 憑證驗證 (verify=False) 以解決連線問題。")

    try:
        # 使用 requests 獲取內容
        # *** 解決 SSLCertVerificationError 錯誤：加入 verify=False ***
        response = requests.get(twse_url, verify=False)
        
        # 檢查 HTTP 狀態碼
        if response.status_code != 200:
            print(f"❌ 網路請求失敗，狀態碼: {response.status_code}")
            return None
        
        # 將內容從 cp950 (Big5 擴充) 解碼為字串，以便 Pandas 讀取
        csv_data_string = response.content.decode('cp950', errors='ignore')
        
        # 使用 StringIO 模擬檔案，讓 Pandas 讀取字串
        data_io = StringIO(csv_data_string)

        # 讀取 CSV。TWSE CSV 通常第一行是描述，第二行才是真正的欄位名稱 (Header)
        df = pd.read_csv(data_io, header=1, encoding='cp950')
        
        # 數據清理步驟：
        
        # 1. 移除最後幾行可能出現的空白或註釋行 (通常會以 NaN 呈現)
        df.dropna(how='all', inplace=True)
        
        # 2. 重新命名欄位為中文 (假設前四個欄位分別是日期、名稱、說明、備註)
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
    """
    將 DataFrame 儲存為本地 CSV 檔案。
    *** 修正編碼為 cp950 以解決本地開啟亂碼問題 ***
    """
    filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "twse_holidays" , f"twse_holidays_{year}.csv")
    
    try:
        # 使用 cp950 儲存，以確保在傳統中文作業系統上開啟時不會亂碼
        df.to_csv(filename, index=False, encoding='cp950')
        print(f"💾 資料已成功儲存至 {filename}")
    except Exception as e:
        print(f"❌ 儲存檔案失敗: {e}")

# 定義一個轉換函式 (此函式在主程式中已不再使用，但保留以防未來需要)
def format_chinese_date(md_str: str) -> str:
    """將 MM/DD 格式字串轉換為中文 M月D日，並移除前導零。"""
    if pd.isna(md_str):
        return ''
    try:
        # 假設輸入是 MM/DD，例如 '02/09'
        month, day = md_str.split('/')
        # 使用 int() 轉換來移除前導零 (e.g., '02' -> 2)
        return f"{int(month)}月{int(day)}日"
    except ValueError:
        return md_str # 如果格式錯誤，則返回原始字串


# --- 主程式執行區 ---
if __name__ == '__main__':
    
    # 抓取 TWSE 休市日期，從2021年(110年開始)，因為TWSE僅提供近幾年的資料
    start_year = 2021
    end_year = date.today().year
    
    for get_year in range(start_year, end_year+1):
        # ----------------------------------------------------
        # 抓取並儲存 TWSE 休市日期資料，每年一次 
        holidays_df = fetch_twse_holidays(get_year)
        save_dataframe_to_csv(holidays_df, get_year)
        
        # ----------------------------------------------------
        # 生成並取得全年度週末日期清單
        full_year_weekend_list = generate_full_year_weekend_dates(get_year)
        
        save_dates_to_csv(full_year_weekend_list, get_year, filename='weekend_calendar.csv') 
        
        # ----------------------------------------------------

        # if holidays_df is not None and not holidays_df.empty:
            
        #     # 1. 提取 '日期' 欄位，並將格式從 YYY/MM/DD 轉換為 MM/DD
        #     # 例如 '113/01/01' 變成 '01/01'
        #     dates_md = holidays_df['日期'].astype(str).str.split('/').str[1:].str.join('/')
            
        #     # 2. 合併設定的年份 (end_year) 與 月/日 (MM/DD)
        #     # 例如 '2024' + '/' + '01/01' 變成 '2024/01/01'
        #     end_year_str = str(end_year)
        #     dates_gregorian = end_year_str + '/' + dates_md
            
        #     # 3. 將 MM/DD 格式轉換為 MMDD 數值字串格式 (例如 '01/01' 變成 '0101')
        #     dates_mmdd = dates_md.str.replace('/', '', regex=False)
            
        #     # 4. 輸出各種格式的資料
        #     print(f"\n--- TWSE 休市日期與全年度週末日期 ({end_year} 年) ---")
            
        #     # 輸出 YYYY/MM/DD 格式
        #     print("\n[A] 休市日 YYYY/MM/DD (完整西元日期):")
        #     print(dates_gregorian.to_string(index=False))
            
        #     # 輸出 MMDD 格式 (回應使用者要求)
        #     print("\n[B] 休市日 MMDD (月日數值字串):")
        #     print(dates_mmdd.to_string(index=False))
            
        #     # 5. 取得休市日的總數量
        #     total_holidays = len(holidays_df)
        #     print(f"\n[C] 休市日的總數量: {total_holidays} 天")

        #     # 6. 輸出全年度日期清單
        #     print("\n[D] 全年度週末日期清單 (前 10 筆):")
        #     if full_year_weekend_list:
        #         # 只顯示前 10 筆，避免輸出過長
        #         for i, date_str in enumerate(full_year_weekend_list[:10]):
        #             print(date_str)
        #         if len(full_year_weekend_list) > 10:
        #             print(f"... 還有 {len(full_year_weekend_list) - 10} 筆日期 (總數: {len(full_year_weekend_list)} 筆)")
        #     else:
        #         print("全年度週末日期清單為空。")
            
        #     print("\n---------------------------------------")

        #     # 儲存結果 (儲存時仍包含原始完整日期資料，以利檔案使用)
        #     save_dataframe_to_csv(holidays_df, end_year)
        # else:
        #     # 僅有抓取TWSE休市資料失敗時，才顯示錯誤訊息
        #     print("無法取得休市日期資料，請檢查網路連線或 TWSE 網址。")
            
        #     # 即使抓取失敗，也要展示全年度日期生成功能
        #     if full_year_weekend_list:
        #         print(f"\n--- 僅展示全年度週末日期清單 ({end_year} 年) ---")
        #         print("[D] 全年度週末日期清單 (前 10 筆):")
        #         for i, date_str in enumerate(full_year_weekend_list):
        #             print(date_str)
        #         if len(full_year_weekend_list) > 10:
        #             print(f"... 還有 {len(full_year_weekend_list) - 10} 筆日期 (總數: {len(full_year_weekend_list)} 筆)")
