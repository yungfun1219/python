import pandas as pd

# ==========================================================
# 【重要】請根據您的檔案內容，修改以下參數：
# ==========================================================
FILE_PATH = r'D:\Python_repo\python\選股投資策略\stock_data\raw\exchange_list.csv'


# 1. 想查詢的證券代號（範例使用台積電 2330，您可以改為任何您想查的代號）
TARGET_CODE = '2607' 

# 2. 檔案中記錄「證券代號」的欄位名稱（請參考您的 CSV 檔案標題行）
CODE_COLUMN_NAME = '有價證券代號' 

# 3. 檔案中記錄「上市日」的欄位名稱（請參考您的 CSV 檔案標題行）
LISTING_DATE_COLUMN_NAME = '上市日' 
# ==========================================================


def get_listing_date_by_code(file_path, target_code, code_col, date_col):
    """
    讀取 CSV 檔案，找出指定證券代號的上市日期。
    """
    print(f"--- 查詢代號 {target_code} 的上市日期 ---")
    
    try:
        # 1. 讀取 CSV 檔案
        df = pd.read_csv(file_path)
        
        # 2. 數據清洗：將代號欄位轉為字串並移除前後空白，確保查找成功
        df[code_col] = df[code_col].astype(str).str.strip() 

        # 3. 篩選出目標代號的資料
        # 使用布林索引 (Boolean Indexing) 進行查找
        target_data = df[df[code_col] == target_code]
        
        if target_data.empty:
            print(f"【錯誤】在檔案中找不到證券代號 '{target_code}' 的資料。")
            print(f"請檢查檔案中實際使用的代號，或欄位名稱 '{code_col}' 是否正確。")
            # 輔助提示：顯示檔案中的所有欄位名稱
            print(f"檔案中的所有欄位名稱為：{df.columns.tolist()}")
            return None

        # 4. 取得上市日期
        # 由於只有一筆資料，直接取該欄位的第一個值
        listing_date = target_data[date_col].iloc[0]
        
        print(f"\n【查詢結果】")
        print(f"證券代號 {target_code} 的上市日為: {listing_date}")
        print("-" * 30)
        
        return listing_date

    except FileNotFoundError:
        print(f"【錯誤】找不到檔案路徑：{file_path}")
    except KeyError as e:
        print(f"【錯誤】指定的欄位名稱錯誤。找不到欄位：{e}")
        # 輔助提示：顯示檔案中的所有欄位名稱
        print(f"檔案中的所有欄位名稱為：{df.columns.tolist()}")
    except Exception as e:
        print(f"發生未知錯誤：{e}")


# 執行函式
found_date = get_listing_date_by_code(
    FILE_PATH, 
    TARGET_CODE, 
    CODE_COLUMN_NAME, 
    LISTING_DATE_COLUMN_NAME
)

print(f"取得的上市日期為: {found_date.replace('/', '')}")