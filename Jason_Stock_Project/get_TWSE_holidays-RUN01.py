### 說明 ###
# 直接在證券交易所TWSE抓取年度休市檔案，通常每年年底才會提供下一年度資料。
# 但抓到到CSV檔案，內容有些會寫-- 最後交易日等相關字眼，所以也不是休市日，
# 所以需手動將此刪除，然後另存檔案，後面增加_OK字樣，
# 之後處理會用此_OK的檔案來進行處理
###########

import pathlib
import pandas as pd
import requests
from io import StringIO
from typing import List
import urllib3

# 禁用 requests 內部使用的 urllib3 的 InsecureRequestWarning 警告
# 這是為了避免在使用 verify=False 時不斷出現警告訊息
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 設定參數 ---
# 1. 將程式的所在位置當作基本資料夾位置 base_directory
BASE_DIR = pathlib.Path(__file__).resolve().parent

# 2. 設定要完成的年度清單
#    若有增加新的年度，就直接年寫在後面即可
TARGET_YEARS: List[int] = [2021, 2022, 2023, 2024, 2025, 2026]

# 4. 定義檔案儲存路徑
OUTPUT_FOLDER = BASE_DIR / "datas" / "twse_holidays"


def check_file_and_create_dir(year: int) -> pathlib.Path:
    """
    檢查輸出檔案是否存在，如果不存在則建立目錄並回傳檔案路徑。

    參數:
        year (int): 目標年份。

    回傳:
        pathlib.Path: 完整的輸出檔案路徑。
    """
    # 建立檔案名稱: twse_holidays_YYYY.csv
    filename = f"twse_holidays_{year}.csv"
    file_path = OUTPUT_FOLDER / filename
    
    # 確保輸出目錄存在 (如果不存在會自動創建)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 5. 檢查資料夾內有無檔案，若已經有檔案，則跳出不抓取資料
    if file_path.is_file():
        print(f"⏩ 跳過 {year} 年：檔案 {filename} 已存在於 {OUTPUT_FOLDER}")
        return None  # 返回 None 表示檔案已存在，應跳過抓取
    
    return file_path


def fetch_and_save_twse_holidays(year: int, save_path: pathlib.Path) -> bool:
    """
    從台灣證券交易所網站抓取指定年份的休市日期資料並儲存。

    參數:
        year (int): 要抓取的年份。
        save_path (pathlib.Path): 儲存 CSV 檔案的完整路徑。

    回傳:
        bool: 抓取並儲存是否成功。
    """
    # 3. TWSE 網址結構
    twse_url = (
        f"https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
        f"?date={year}0101&response=csv"
    )
    
    print(f"📡 正在抓取 {year} 年 TWSE 休市資料...")
 
    try:
        # 使用 requests 獲取內容，並設置 verify=False 解決可能的 SSL 憑證錯誤
        response = requests.get(twse_url, verify=False, timeout=10)
        
        if response.status_code != 200:
            print(f"❌ 網路請求失敗，狀態碼: {response.status_code}")
            return False
        
        # TWSE 網站使用 cp950 編碼 (Big5 擴充)
        csv_data_string = response.content.decode('cp950', errors='ignore')
        
        # 使用 StringIO 模擬檔案，讓 Pandas 讀取字串
        data_io = StringIO(csv_data_string)

        # 讀取 CSV。TWSE CSV 通常第一行是描述，第二行才是真正的欄位名稱 (Header)
        # 設置 header=1 讓 Pandas 以第二行 (索引 1) 作為欄位名稱
        df = pd.read_csv(data_io, header=1, encoding='cp950')
        
        # 清理資料：移除最後幾行可能出現的空白或註釋行
        df.dropna(how='all', inplace=True)
        
        # 由於我們沒有使用 try-except 的方式讀取，可能需要手動重新命名或檢查欄位
        if len(df.columns) >= 4:
             df.columns = ['日期', '名稱', '說明', '備註']
        
        # 儲存為 CSV 檔案，使用 cp950 編碼，確保在傳統中文環境下開啟不亂碼
        df.to_csv(save_path, index=False, encoding='cp950')
        
        print(f"✅ 成功抓取並儲存 {year} 年休市資料。共 {len(df)} 筆。")
        print(f"   儲存路徑: {save_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ 網路連線錯誤 (TWSE): {e}")
        return False
    except Exception as e:
        print(f"❌ 處理或儲存資料時發生錯誤: {e}")
        return False


# --- 主程式執行區 ---
if __name__ == '__main__':
    
    print(f"--- TWSE 假日資料抓取 (目標年度: {TARGET_YEARS}) ---")
    print(f"基礎目錄: {BASE_DIR}")
    print(f"輸出目錄: {OUTPUT_FOLDER}\n")
    
    
    successful_count = 0
    
    for year in TARGET_YEARS:
        print(f"\n{'='*40}")
        print(f"處理年度: {year}")
        print(f"{'='*40}")
        
        # 檢查檔案是否存在，並取得儲存路徑 (如果檔案不存在)
        file_to_save = check_file_and_create_dir(year)
        
        if file_to_save:
            # 檔案不存在，執行抓取和儲存
            if fetch_and_save_twse_holidays(year, file_to_save):
                successful_count += 1
        
    print(f"\n--- 總結 ---")
    print(f"🎯 目標處理 {len(TARGET_YEARS)} 個年度。")
    print(f"✅ 成功抓取/更新 {successful_count} 個年度。")
    print("🎉 所有作業完成。")