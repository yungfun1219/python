import pandas as pd
from pathlib import Path
import os
import re
from datetime import datetime

# 檔案路徑與年份設定
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CURRENT_YEAR = 2025 # 用於推斷中文日期格式的年份

# 輸入與輸出檔案路徑
INPUT_FILE_NAME = f"twse_holidays_{CURRENT_YEAR}.csv"
OUTPUT_FILE_NAME = f"twse_holidays_{CURRENT_YEAR}_cleaned.csv"

# 輸入/輸出資料夾位於 datas/twse_holidays
FILE_FOLDER = BASE_DIR / "datas" / "twse_holidays"
INPUT_FILE_PATH = FILE_FOLDER / INPUT_FILE_NAME
OUTPUT_FILE_PATH = FILE_FOLDER / OUTPUT_FILE_NAME


# --- 新增 Helper function 1: 處理中文日期格式 ---
def convert_chinese_date(date_str: str) -> str:
    """將【M月D日】格式轉換為 YYYY/M/D 格式，使用 CURRENT_YEAR 作為年份。"""
    if not isinstance(date_str, str):
        return date_str
    # 嘗試匹配 【1月22日】 或 【12月1日】 這種格式
    match = re.match(r'(\d{1,2})月(\d{1,2})日', date_str.strip())
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        # 轉換為 YYYY/M/D 格式，例如 2025/1/22
        return f"{CURRENT_YEAR}/{month}/{day}" 
        
    return date_str # 如果不匹配，則返回原字串

# --- 修正 Helper function 2: 處理 YYYY/M/D 格式與民國年 ---
def format_date_no_leading_zeros(date_str: str) -> str:
    """將日期字串轉換為 YYYY/M/D 格式 (無前導零)，並處理民國年 (ROC Year) 轉換。"""
    if not date_str:
        return date_str
    
    # 1. 嘗試匹配 'ROC_YEAR/M/D' 或 'YYYY/M/D' 格式
    # 匹配 2到4位數字 (年) / 1到2位數字 (月) / 1到2位數字 (日)，並接受 / 或 - 作為分隔符
    match = re.match(r'(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})', date_str.strip())
    
    if match:
        try:
            year_part = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            
            # 處理民國年 (ROC Year)：如果年份是 100-1911 之間，則加上 1911 轉換為西元年
            # 這是 TWSE 數據常見的格式 (e.g., 114 + 1911 = 2025)
            if 100 <= year_part < 1911:
                year = year_part + 1911
            else: 
                # 假設其他情況 (如 2025, 2024) 為西元年
                year = year_part
                
            # 輸出為 YYYY/M/D 格式 (Python f-string 自動處理整數轉字串時的前導零移除)
            return f"{year}/{month}/{day}"
            
        except Exception:
            # 如果數字轉換或邏輯有問題，則保留原字串
            return date_str 

    # 2. 如果不匹配標準 ROC/Gregorian 格式，則嘗試通用解析 (保留原有邏輯)
    try:
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.isna(dt):
            return date_str
        
        return f"{dt.year}/{dt.month}/{dt.day}"
    except Exception:
        return date_str


def clean_and_save_holidays():
    """
    讀取原始 CSV 檔案，執行日期格式清理和轉換，並儲存到新的 CSV 檔案。
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
        
        # 清理欄位名稱
        df.columns = df.columns.str.strip()

        if '日期' not in df.columns:
            print(f"【錯誤】檔案中找不到 '日期' 欄位。檔案欄位為: {df.columns.tolist()}")
            return

        # 0. (新增步驟) 處理中文特殊日期格式 (如 【1月22日】)
        # 這一步應在移除括號之前執行，確保能正確匹配中文格式
        df['日期'] = df['日期'].astype(str).apply(convert_chinese_date)
        
        # 1. 清理日期欄位：移除括號及其內容（例如：移除 "(三)"）
        # 正則表達式: 尋找並替換 0 個或多個空格，後接括號及其中的任何內容
        df['日期'] = df['日期'].astype(str).str.replace(r'\s*\([^)]*\)\s*', '', regex=True).str.strip()
        df['日期'] = df['日期'].astype(str).apply(convert_chinese_date)
        
        # 2. 標準化並轉換為 YYYY/M/D 格式 (無前導零)
        # 修正後的函數同時處理了西元年/民國年轉換和前導零移除
        df['日期'] = df['日期'].apply(format_date_no_leading_zeros)
        
        # 3. 驗證清理結果 (可選，用於偵錯)
        test_dates = pd.to_datetime(df['日期'], errors='coerce')
        
        if test_dates.isna().any() and not df.empty:
            invalid_dates_count = test_dates.isna().sum()
            print(f"【警告】有 {invalid_dates_count} 筆日期在清理和格式化後仍無法解析。")
            
            invalid_indices = df.index[test_dates.isna()].tolist()
            if invalid_indices:
                print(f"請檢查原始 CSV 檔案中這些日期欄位 (前 3 筆索引: {invalid_indices[:3]})。")
        else:
            print(" [偵錯] 所有清理後的日期皆可成功解析。")

        # 4. 儲存清理後的檔案
        df.to_csv(OUTPUT_FILE_PATH, index=False, encoding='utf-8-sig')
        
        print(f"✅ 清理完成！已儲存至新檔案: {OUTPUT_FILE_NAME}")
        print(f" 新檔案路徑: {OUTPUT_FILE_PATH.relative_to(BASE_DIR)}")
        print(f" 檔案總筆數: {len(df)}")
        
    except Exception as e:
        print(f"【重大錯誤】清理檔案時發生錯誤: {e}")

if __name__ == '__main__':
    clean_and_save_holidays()