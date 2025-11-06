import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import urllib3
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import sys
import pathlib
from pathlib import Path

from dotenv import load_dotenv # ➊ 匯入Line機器人函式庫


# 從 Excel 檔案中讀取股票庫存，將其另存為 CSV 檔案。
def extract_excel_sheet_filter_and_save(excel_file_path: str, sheet_name: str, filter_column: str, filter_value: any, output_dir: str = None) -> Path:
    """
    從指定的 Excel 檔案中讀取特定工作表，跳過第二行，篩選資料後，將其另存為 CSV 檔案。

    Args:
        excel_file_path (str): 原始 Excel 檔案的完整路徑。
        sheet_name (str): 要讀取的工作表名稱 (例如: '股票庫存統計')。
        filter_column (str): 要進行篩選的欄位名稱 (例如: '目前股數庫存統計')。
        filter_value (any): 要篩除的值。
        output_dir (str, optional): CSV 檔案的儲存目錄。如果為 None，則儲存在原始檔案的目錄。

    Returns:
        Path: 儲存成功的 CSV 檔案路徑。
    """
    
    original_path = Path(excel_file_path)
    
    if not original_path.exists():
        raise FileNotFoundError(f"錯誤：找不到 Excel 檔案在路徑：{excel_file_path}")

    print(f"✅ 正在讀取 Excel 檔案：{original_path.name}")
    print(f"🎯 目標工作表名稱：{sheet_name}")

    try:
        # 1. 讀取 Excel 中指定工作表的資料
        # header=0: 指定 Excel 的第一行（索引 0）作為欄位名稱
        # skiprows=[1]: 跳過索引為 1 的行，即 Excel 中的第二行
        df = pd.read_excel(
            original_path, 
            sheet_name=sheet_name, 
            header=0,
            skiprows=[1]  # <--- ❗ 這裡加入跳過 Excel 第二行（索引 1）的設定
        )
        
        if df.empty:
            print(f"警告：工作表 '{sheet_name}' 讀取到的數據為空。")
            return None

    except ValueError as e:
        raise ValueError(f"錯誤：在 Excel 檔案中找不到名為 '{sheet_name}' 的工作表。請檢查名稱是否正確。詳細錯誤: {e}")
    except Exception as e:
        raise Exception(f"讀取 Excel 檔案時發生錯誤：{e}")
        
    # 2. **【關鍵篩選步驟】**
    if filter_column not in df.columns:
        print(f"警告：找不到篩選欄位 '{filter_column}'。跳過篩選步驟。")
    else:
        initial_rows = len(df)
        print(f"\n🔍 開始篩選：移除 '{filter_column}' 值為 '{filter_value}' 的資料...")
        
        # 嘗試將篩選欄位轉換為數值類型，coerce 會將非數值轉換為 NaN
        df[filter_column] = pd.to_numeric(df[filter_column], errors='coerce')
        
        # 篩選邏輯：保留 '目前股數庫存統計' 不等於 0 的行
        df_filtered = df[df[filter_column] != float(filter_value)]
        
        removed_rows = initial_rows - len(df_filtered)
        print(f"  -> 原始筆數 (已跳過第二行): {initial_rows} 筆")
        print(f"  -> 移除筆數: {removed_rows} 筆")
        print(f"  -> 剩餘筆數: {len(df_filtered)} 筆")
        
        df = df_filtered
        
        if df.empty:
            print("警告：篩選後數據為空。")
            return None


    # 3. 準備輸出 CSV 檔案的路徑
    
    if output_dir is None:
        output_dir = original_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
    csv_file_name = f"{sheet_name}_filtered{timestamp}.csv"
    output_csv_path = output_dir / csv_file_name
    
    # 4. 儲存為 CSV 檔案
    df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

    return output_csv_path

#從指定的 CSV 檔案中讀取並回傳所有證券代號的列表。
def get_all_stock_codes(file_path, code_col_name):
    """
    從指定的 CSV 檔案中讀取並回傳所有證券代號的列表。
    """
    print(f"--- 開始讀取所有證券代號清單：{os.path.basename(file_path)} ---")
    
    if not os.path.exists(file_path):
        print(f"【錯誤】找不到檔案路徑：{file_path}")
        return []

    try:
        df = pd.read_csv(file_path, dtype={code_col_name: str})
        
        if code_col_name not in df.columns:
            raise KeyError(f"指定的欄位名稱 '{code_col_name}' 不存在於檔案中。")
            
        # 提取欄位並轉為列表，並去除空白和空值
        stock_codes_list = df[code_col_name].str.strip().dropna().tolist()
        
        print(f"【成功】總共取得 {len(stock_codes_list)} 個證券代號準備處理。")
        return stock_codes_list

    except Exception as e:
        print(f"【錯誤】讀取或處理股票清單時發生錯誤：{e}")
        return []
    

if __name__ == '__main__':
    
    BASE_DIR = pathlib.Path(__file__).resolve().parent 
    LIST_FILE_PATH = BASE_DIR / "datas" / "股票分析.xlsx"
    RAW_DATA_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"
    sheet_name = '股票庫存統計'
    
    # 批量處理時，此變數將被清單中的代號取代
    TARGET_CODE = '1101' 

    
    CODE_COL = '證券名稱'
    
    code_all = get_all_stock_codes(LIST_FILE_PATH, CODE_COL)
    
  
    final_csv_path = extract_excel_sheet_filter_and_save(
        excel_file_path=LIST_FILE_PATH,
        sheet_name=sheet_name,
        filter_column="上市/櫃",
        filter_value="上市",
        output_dir= RAW_DATA_DIR

    )
    print("=== 主要程式執行結束 ===")
    print(final_csv_path)
    
    