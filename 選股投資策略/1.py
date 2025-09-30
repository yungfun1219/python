import pandas as pd
import numpy as np # 建議匯入以備不時之需，但此處非必需

# 必須先定義這些參數，否則程式會因為變數未定義而停止
file_path = r"D:\GitHub-Repo\python\選股投資策略\exchange_list.csv" # **請修改為你的檔案路徑！**
code_col = "有價證券代號"         # **請修改為你檔案中實際的代號欄位名稱！**
target_code = "2330"         # **請修改為你要查詢的代號！**
date_col = "上市日"          # **請修改為你檔案中實際的日期欄位名稱！**


def get_listing_date(file_path, code_col, target_code, date_col):
    """
    從 CSV 檔案中查找特定證券代號的上市日期。
    """
    df = None  # 初始化 df，以防在 KeyError 中被引用時還未定義
    try:
        # 1. 讀取 CSV 檔案
        # 建議加入 encoding='utf-8' 以避免中文亂碼問題
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 2. 數據清洗：將代號欄位轉為字串並移除前後空白，確保查找成功
        # 判斷欄位是否存在，如果不存在會自動跳到 KeyError
        df[code_col] = df[code_col].astype(str).str.strip() 

        # 3. 篩選出目標代號的資料
        target_code_str = str(target_code).strip() # 確保查找的目標也是字串並移除空白
        # 使用布林索引 (Boolean Indexing) 進行查找
        target_data = df[df[code_col] == target_code_str]
        
        if target_data.empty:
            print(f"【錯誤】在檔案中找不到證券代號 '{target_code_str}' 的資料。")
            print(f"請檢查檔案中實際使用的代號，或欄位名稱 '{code_col}' 是否正確。")
            # 輔助提示：顯示檔案中的所有欄位名稱
            print(f"檔案中的所有欄位名稱為：{df.columns.tolist()}")
            return None

        # 4. 取得上市日期
        # 由於只有一筆資料，直接取該欄位的第一個值
        listing_date = target_data[date_col].iloc[0]
        
        print(f"\n【查詢結果】")
        print(f"證券代號 {target_code_str} 的上市日為: {listing_date}")
        print("-" * 30)
        
        return listing_date

    except FileNotFoundError:
        print(f"【錯誤】找不到檔案路徑：{file_path}")
    except KeyError as e:
        print(f"【錯誤】指定的欄位名稱錯誤。找不到欄位：{e}")
        # 輔助提示：如果 df 已經被定義，則顯示欄位名稱
        if df is not None:
             print(f"檔案中的所有欄位名稱為：{df.columns.tolist()}")
        else:
             print("由於讀取檔案失敗，無法顯示欄位名稱。")
    except Exception as e:
        print(f"發生未知錯誤：{e}")
    
    return listing_date

# 執行函式
result = get_listing_date(file_path, code_col, target_code, date_col)
print(result)

print(f"取得的上市日期為: {result.replace('/', '')}")

start_date = result.replace('/', '')
start_date_year = start_date[0:3]
start_date_month = start_date[4:6]
start_date_day = start_date[7:9]

while int(start_date[4:6]):
    if i == '0':
        start_date = start_date.replace(i, '')
    else:
        break