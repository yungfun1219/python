import pandas as pd
import os
from typing import Union

# 檢查是否為休市日
def check_date_in_csv(file_path: str, date_to_check: str, date_column_name: str = '日期') -> Union[bool, pd.Series]:
    """
    檢查指定日期字串是否出現在 CSV 檔案的特定欄位中。
    
    Args:
        file_path (str): holidays_all.csv 檔案的完整路徑。
        date_to_check (str): 要檢查的日期字串，例如 '2025/10/10'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
        
    Returns:
        Union[bool, pd.Series]: 如果找到，返回包含匹配行的 Series (布林值)，
                                 如果未找到或檔案不存在，返回 False。
    """
    print(f"🔍 正在檢查檔案: {os.path.basename(file_path)}")
    print(f"目標日期: {date_to_check}")

    if not os.path.exists(file_path):
        print("【錯誤】檔案路徑不存在，請確認路徑是否正確。")
        return False
        
    try:
        # 由於 holidays_all.csv 是由您程式碼最後儲存的，
        # 且您儲存時使用 encoding='utf-8-sig'，這裡也使用相同的編碼讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            print(f"檔案中的欄位有: {df.columns.tolist()}")
            return False

        # 使用向量化操作 (isin) 檢查欄位中是否包含目標日期
        # 即使欄位類型是 object (字串)，也能正確檢查
        is_present = df[date_column_name].isin([date_to_check])
        
        if is_present.any():
            # 找到匹配的行
            matched_rows = df[is_present]
            print(f"✅ 日期 '{date_to_check}' 已在檔案中找到！")
            print("--- 匹配的資料列 ---")
            print(matched_rows)
            return True
        else:
            print(f"❌ 日期 '{date_to_check}' 未在檔案的 '{date_column_name}' 欄位中找到。")
            return False

    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return False
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return False

# ----------------------------------------------------------------------
# 範例執行
# ----------------------------------------------------------------------

# 您指定的檔案路徑
FILE_PATH = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays\holidays_all.csv"
DATE_TO_CHECK = '2025/10/13' 
DATE_COLUMN = '日期' # 根據您前面的程式碼，合併後的欄位名稱應為 '日期'

# 執行檢查
result_found = check_date_in_csv(FILE_PATH, DATE_TO_CHECK, DATE_COLUMN)

print(result_found)
# 顯示最後一天
df = pd.read_csv(FILE_PATH, encoding='utf-8-sig')
print(df.iloc[-1])
# 為了在沒有您實際檔案的情況下演示結果，我將模擬執行流程和輸出：
# 由於我無法存取您的本地檔案系統，我無法給出最終的布林結果 (True/False)。
# 請您在您的環境中執行此函式。

#print(result_found.iloc[-1])
# --- 模擬檔案內容 (假設 holidays_all.csv 包含以下數據) ---
"""
日期,假日名稱
2025/01/01,元旦
2025/10/10,國慶日
2025/12/25,聖誕節
"""

# print("\n--- 模擬執行結果 (假設檔案存在且包含數據) ---")
# # 如果檔案存在且包含 '2025/10/10,國慶日'
# # 輸出會是:
# # ✅ 日期 '2025/10/10' 已在檔案中找到！
# # --- 匹配的資料列 ---
# #          日期 假日名稱
# # 1  2025/10/10    國慶日

# print(f"\n請執行 check_date_in_csv('{FILE_PATH}', '{DATE_TO_CHECK}') 來取得實際結果。")

# 假設檔案不存在的情況 (模擬輸出)
# check_date_in_csv(r"C:\NonExistentFile.csv", '2025/10/10', '日期')
# 輸出: 【錯誤】檔案路徑不存在，請確認路徑是否正確。

#print(f"\n請使用您環境中的檔案路徑執行 check_date_in_csv('{FILE_PATH}', '{DATE_TO_CHECK}') 以取得實際結果。")