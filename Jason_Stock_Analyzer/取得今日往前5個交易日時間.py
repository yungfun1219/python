import pandas as pd
from pathlib import Path
import os
import re

# 檔案路徑與年份設定
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CURRENT_YEAR = 2025 

# 輸入與輸出檔案路徑
INPUT_FILE_NAME = f"twse_holidays_{CURRENT_YEAR}.csv"
OUTPUT_FILE_NAME = f"twse_holidays_{CURRENT_YEAR}_cleaned.csv"

# 輸入/輸出資料夾位於 datas/twse_holidays
FILE_FOLDER = BASE_DIR / "datas" / "twse_holidays"
INPUT_FILE_PATH = FILE_FOLDER / INPUT_FILE_NAME
OUTPUT_FILE_PATH = FILE_FOLDER / OUTPUT_FILE_NAME

# Helper function to enforce YYYY/M/D format (without leading zeros)
def format_date_no_leading_zeros(date_str: str) -> str:
    """將日期字串轉換為 YYYY/M/D 格式 (無前導零)。"""
    if not date_str:
        return date_str
    try:
        # 嘗試將字串解析為 datetime object
        # 修正：顯式嘗試解析最可能出現的格式 'YYYY/MM/DD'
        dt = pd.to_datetime(date_str, format='%Y/%m/%d', errors='coerce')
        
        # 如果顯式解析失敗，則嘗試使用自動推斷
        if pd.isna(dt):
            dt = pd.to_datetime(date_str, errors='coerce')

        if pd.isna(dt):
            return date_str # 無法解析，保留原樣 (將在後續步驟中被 dropna 移除)
        
        # 提取年、月、日，並轉換為整數再轉回字串以移除前導零
        year = dt.year
        month = dt.month
        day = dt.day
        
        # 輸出為 YYYY/M/D 格式 (Python f-string 自動處理整數轉字串時的前導零移除)
        return f"{year}/{month}/{day}"
    except Exception:
        return date_str

def clean_and_save_holidays():
    """
    讀取原始 CSV 檔案，移除日期欄位中括號內的星期幾資訊，
    並將清理後的資料儲存到新的 CSV 檔案。
    """
    print(f"\n--- 正在執行 {CURRENT_YEAR} 年特殊假日檔案清理 ---")
    print(f"原始輸入檔案: {INPUT_FILE_NAME}")

    if not INPUT_FILE_PATH.exists():
        print(f"【錯誤】找不到輸入檔案: {INPUT_FILE_PATH.relative_to(BASE_DIR)}")
        print("請確認檔案路徑和名稱是否正確。")
        return

    try:
        # 使用 Big5 編碼讀取檔案，以解決先前遇到的編碼問題
        df = pd.read_csv(INPUT_FILE_PATH, encoding='big5')
        
        # 清理欄位名稱，移除可能存在的隱藏空格
        df.columns = df.columns.str.strip()

        if '日期' not in df.columns:
            print(f"【錯誤】檔案中找不到 '日期' 欄位。檔案欄位為: {df.columns.tolist()}")
            return

        # 1. 清理日期欄位：移除括號及其內容（例如：移除 "(三)"）
        # 正則表達式: 尋找並替換 0 個或多個空格，後接括號及其中的任何內容
        df['日期'] = df['日期'].astype(str).str.replace(r'\s*\([^)]*\)\s*', '', regex=True).str.strip()
        
        # 2. 標準化並轉換為 YYYY/M/D 格式 (無前導零)
        # 使用自定義函數處理，確保輸出為 YYYY/M/D 格式
        df['日期'] = df['日期'].apply(format_date_no_leading_zeros)

        # 3. 驗證清理結果 (可選，用於偵錯)
        # 檢查清理後的日期是否符合預期的 YYYY/M/D 格式，這裡使用一個較寬鬆的解析來檢查有效性
        test_dates = pd.to_datetime(df['日期'], errors='coerce')
        
        # 如果有任何日期無法解析為有效日期 (NaT)，則發出警告
        if test_dates.isna().any() and not df.empty:
            invalid_dates_count = test_dates.isna().sum()
            print(f"【警告】有 {invalid_dates_count} 筆日期在清理和格式化後仍無法解析。")
            
            # 找到並列印無法解析的前幾筆原始日期，以便用戶檢查 CSV 原始格式
            invalid_indices = df.index[test_dates.isna()].tolist()
            if invalid_indices:
                print(f"請檢查原始 CSV 檔案中這些日期欄位 (前 3 筆索引: {invalid_indices[:3]})。")
        else:
            print(" [偵錯] 所有清理後的日期皆可成功解析。")


        # 4. 儲存清理後的檔案
        # 儲存時使用 utf-8-sig 編碼，確保後續 Python 程式可以通用讀取
        df.to_csv(OUTPUT_FILE_PATH, index=False, encoding='utf-8-sig')
        
        print(f"✅ 清理完成！已儲存至新檔案: {OUTPUT_FILE_NAME}")
        print(f" 新檔案路徑: {OUTPUT_FILE_PATH.relative_to(BASE_DIR)}")
        print(f" 檔案總筆數: {len(df)}")
        
    except Exception as e:
        print(f"【重大錯誤】清理檔案時發生錯誤: {e}")

if __name__ == '__main__':
    clean_and_save_holidays()
