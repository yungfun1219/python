import pandas as pd
from typing import Optional
import os

# 檔案的絕對路徑 (請根據您的實際路徑調整)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_FILE_PATH = os.path.join(BASE_DIR, "datas", "股票分析.xlsx")

def get_inventory_stocks(file_path: str) -> Optional[pd.DataFrame]:
    """
    從【股票庫存統計】工作表讀取資料，並篩選出「目前股數庫存統計」大於 0 的記錄。

    Args:
        file_path: Excel 檔案的完整路徑。

    Returns:
        包含 (證券名稱, 產業別, 購買均價) 的 DataFrame，如果失敗則返回 None。
    """
    SHEET_NAME = "股票庫存統計"
    FILTER_COLUMN = "目前股數庫存統計"
    # 需要讀取所有相關欄位才能篩選
    COLUMNS_TO_READ = ["證券名稱", "產業別", "購買均價", FILTER_COLUMN]
    COLUMNS_TO_DISPLAY = ["證券名稱", "產業別", "購買均價"]
    
    print(f"--- 正在處理工作表: {SHEET_NAME} ---")
    
    try:
        # 1. 讀取指定工作表和所有相關欄位
        df_inventory_raw = pd.read_excel(
            file_path,
            sheet_name=SHEET_NAME,
            usecols=COLUMNS_TO_READ
        )
        
        # 2. 數據清洗與篩選
        
        # 確保篩選欄位是數字類型，以便進行數值比較 (errors='coerce' 會將非數字轉換為 NaN)
        df_inventory_raw[FILTER_COLUMN] = pd.to_numeric(
            df_inventory_raw[FILTER_COLUMN], 
            errors='coerce'
        )
        
        # 執行篩選邏輯：只保留庫存 > 0 的行
        df_inventory_filtered = df_inventory_raw[
            df_inventory_raw[FILTER_COLUMN] > 0
        ].copy()
        
        # 3. 只保留最終需要的欄位
        df_inventory = df_inventory_filtered[COLUMNS_TO_DISPLAY]

        print(f"✅ {SHEET_NAME} 處理成功。庫存 > 0 的股票數: {len(df_inventory)} 筆。")
        return df_inventory

    except FileNotFoundError:
        print(f"❌ 錯誤：找不到檔案路徑 {file_path}。")
        return None
    except KeyError as e:
        print(f"❌ 錯誤：工作表 '{SHEET_NAME}' 或欄位 '{e.args[0]}' 不存在。請檢查名稱是否正確。")
        return None
    except Exception as e:
        print(f"❌ 讀取 {SHEET_NAME} 時發生未預期的錯誤: {e}")
        return None


def get_followed_stocks(file_path: str) -> Optional[pd.DataFrame]:
    """
    從【關注的股票】工作表讀取證券名稱和產業別。

    Args:
        file_path: Excel 檔案的完整路徑。

    Returns:
        包含 (證券名稱, 產業別) 的 DataFrame，如果失敗則返回 None。
    """
    SHEET_NAME = "關注的股票"
    COLUMNS_TO_READ = ["證券名稱", "產業別"]
    
    print(f"\n--- 正在處理工作表: {SHEET_NAME} ---")

    try:
        # 1. 讀取指定工作表和欄位
        df_followed = pd.read_excel(
            file_path,
            sheet_name=SHEET_NAME,
            usecols=COLUMNS_TO_READ
        )
        
        print(f"✅ {SHEET_NAME} 處理成功。總共關注 {len(df_followed)} 筆股票。")
        return df_followed

    except FileNotFoundError:
        print(f"❌ 錯誤：找不到檔案路徑 {file_path}。")
        return None
    except KeyError as e:
        print(f"❌ 錯誤：工作表 '{SHEET_NAME}' 或欄位 '{e.args[0]}' 不存在。請檢查名稱是否正確。")
        return None
    except Exception as e:
        print(f"❌ 讀取 {SHEET_NAME} 時發生未預期的錯誤: {e}")
        return None


if __name__ == "__main__":
    
    print("--- 開始讀取 Excel 股票數據 ---")
    
    # 呼叫函式 1: 庫存股票
    inventory_df = get_inventory_stocks(EXCEL_FILE_PATH)
    
    # 呼叫函式 2: 關注股票
    followed_df = get_followed_stocks(EXCEL_FILE_PATH)
    
    print("\n--- 讀取結果摘要 ---")
    
    if inventory_df is not None:
        print("【庫存股票 (庫存>0)】")
        print(inventory_df["證券名稱"].to_string(index = False))
    
    if followed_df is not None:
        print("\n【關注股票】")
        print(followed_df)
        
    print("\n--- Excel 數據處理完畢 ---")