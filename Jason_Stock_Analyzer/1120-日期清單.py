import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
import pathlib     # as pathlib

# 設定 CSV 檔案的目標路徑
# 注意：在實際環境中，您可能需要根據您的專案結構調整此路徑。
Now_time_year = datetime.now().strftime("%Y")  #取得目前系統時間的「年」
CSV_FILE_PATH = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-{Now_time_year}.csv"

def _create_mock_csv(filepath: str):
    """
    建立一個模擬的 CSV 檔案，包含 2021 年到 2025 年的日期。
    此功能僅為示範程式碼的讀取部分而存在。
    """
    try:
        start_date = datetime(2021, 1, 4)
        # 假設截止日期為 2025/11/30 (包含今天和未來幾天的日期，以確保篩選邏輯能運作)
        end_date = datetime(2025, 11, 30) 
        
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
            
        df = pd.DataFrame(dates, columns=['Date'])
        
        # 確保目錄存在
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # 寫入模擬 CSV 檔案
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"【初始化】模擬 CSV 檔案已建立於: {filepath}")
    except Exception as e:
        print(f"【初始化】建立模擬 CSV 失敗: {e}")


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
    
    if current_time >= cutoff_time:
        # 21點以後 (含 21:00:00): 截止日為今天
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


# --- 執行主程式 ---
if __name__ == "__main__":
    
    # 1. 確保模擬 CSV 檔案存在 (請在實際運行時替換為您的真實檔案)
    _create_mock_csv(CSV_FILE_PATH)
    
    # 2. 執行日期清單生成
    final_date_list = get_date_list_based_on_time(CSV_FILE_PATH)

    if final_date_list:
        print("\n[輸出的日期清單範例]")
        print(f"前 5 筆: {final_date_list[:5]}")
        print(f"後 5 筆: {final_date_list[-5:]}")