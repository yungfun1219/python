import pandas as pd
import os
from typing import Union
from datetime import datetime, timedelta

# 檢查是否為交易日，若是則回傳True，否則回傳"False"，下一個交易日
def check_next_date_in_csv(file_path: str, date_to_check: str, date_column_name: str = '日期') -> Union[bool, pd.Series]:
    """
    檢查指定日期字串是否出現在 CSV 檔案的特定欄位中。
    Args:
        file_path (str): holidays_all.csv 檔案的完整路徑。
        date_to_check (str): 要檢查的日期字串，例如 '2025/10/10'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[bool, pd.Series]: 如果找到，返回包含匹配行的 Series (布林值)，
                                如果未找到或檔案不存在，返回 False。返回下個交易日的 Series (布林值)，
    """
    #print(f"🔍 正在檢查檔案: {os.path.basename(file_path)}")
    #print(f"目標日期: {date_to_check}")

    if not os.path.exists(file_path):
        print("【錯誤】檔案路徑不存在，請確認路徑是否正確。")
        return False, None
        
    try:
        # 且您儲存時使用 encoding='utf-8-sig'，這裡也使用相同的編碼讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            print(f"檔案中的欄位有: {df.columns.tolist()}")
            return False, None
        
        # 使用向量化操作 (isin) 檢查欄位中是否包含目標日期
        # 即使欄位類型是 object (字串)，也能正確檢查
        
        date_format = '%Y/%m/%d'
        current_date = datetime.strptime(date_to_check, date_format)
        one_day = timedelta(days=1)
        date_to_check_save = date_to_check
        check_next_day = True
        while check_next_day:
            
            is_present = df[date_column_name].isin([date_to_check])
            #print("測試1:date_column_name:", date_to_check)
            
            if is_present.any():
                # 找到匹配的行
                matched_rows = df[is_present]
                print(f"✅ 日期 '{date_to_check}' 為交易日！")
                #print("--- 匹配的資料列 ---")
                #print(matched_rows)
                check_next_day = False
            
            else:
                print(f"✅ 日期 '{date_to_check}' 休市日！")
                tomorrow_date = current_date + one_day
                tomorrow_date_str = tomorrow_date.strftime(date_format)
                #print("測試2:date_column_name:", tomorrow_date_str)    
                date_to_check = tomorrow_date_str
                current_date = tomorrow_date
                check_next_day = True
                
        if date_to_check_save == current_date.strftime(date_format):
            print(f"今天日期: {date_to_check_save} 為交易日")
            return True, date_to_check_save
        else:
            print(f"今天日期: {date_to_check_save} 為休市日")
            print(f"下一個交易日: {current_date.strftime(date_format)}")
            return False, current_date.strftime(date_format)
        
    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return False, None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return False, None

# 檢查是否為交易日，若是則回傳True，否則回傳"False"，上一個交易日
def check_pre_date_in_csv(file_path: str, date_to_check: str, date_column_name: str = '日期') -> Union[bool, pd.Series]:
    """
    檢查指定日期字串是否出現在 CSV 檔案的特定欄位中。
    Args:
        file_path (str): holidays_all.csv 檔案的完整路徑。
        date_to_check (str): 要檢查的日期字串，例如 '2025/10/10'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[bool, pd.Series]: 如果找到，返回包含匹配行的 Series (布林值)，
                                如果未找到或檔案不存在，返回 False。返回下個交易日的 Series (布林值)，
    """
    #print(f"🔍 正在檢查檔案: {os.path.basename(file_path)}")
    #print(f"目標日期: {date_to_check}")

    if not os.path.exists(file_path):
        print("【錯誤】檔案路徑不存在，請確認路徑是否正確。")
        return False, None
        
    try:
        # 且您儲存時使用 encoding='utf-8-sig'，這裡也使用相同的編碼讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            print(f"檔案中的欄位有: {df.columns.tolist()}")
            return False, None
        
        # 使用向量化操作 (isin) 檢查欄位中是否包含目標日期
        # 即使欄位類型是 object (字串)，也能正確檢查
        
        date_format = '%Y/%m/%d'
        current_date = datetime.strptime(date_to_check, date_format)
        one_day = timedelta(days=1)
        date_to_check_save = date_to_check
        check_next_day = True
        while check_next_day:
            
            is_present = df[date_column_name].isin([date_to_check])
            #print("測試1:date_column_name:", date_to_check)
            
            if is_present.any():
                # 找到匹配的行
                matched_rows = df[is_present]
                print(f"✅ 日期 '{date_to_check}' 為交易日！")
                #print("--- 匹配的資料列 ---")
                #print(matched_rows)
                check_next_day = False
            
            else:
                print(f"✅ 日期 '{date_to_check}' 休市日！")
                tomorrow_date = current_date - one_day
                tomorrow_date_str = tomorrow_date.strftime(date_format)
                #print("測試2:date_column_name:", tomorrow_date_str)    
                date_to_check = tomorrow_date_str
                current_date = tomorrow_date
                check_next_day = True
                
        if date_to_check_save == current_date.strftime(date_format):
            print(f"今天日期: {date_to_check_save} 為交易日")
            return True, date_to_check_save
        else:
            print(f"今天日期: {date_to_check_save} 為休市日")
            print(f"上一個交易日: {current_date.strftime(date_format)}")
            return False, current_date.strftime(date_format)
        
    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return False, None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return False, None


# ----------------------------------------------------------------------
# 範例執行
# ----------------------------------------------------------------------

# 您指定的檔案路徑
FILE_PATH = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays\trading_day_2021-2025.csv"
DATE_TO_CHECK = '2025/11/15' 
#DATE_COLUMN = '日期' # 根據您前面的程式碼，合併後的欄位名稱應為 '日期'

# 執行檢查

result_found_next = check_next_date_in_csv(FILE_PATH, DATE_TO_CHECK)

result_found_pre = check_pre_date_in_csv(FILE_PATH, DATE_TO_CHECK)

#result_found = check_date_in_csv(FILE_PATH, DATE_TO_CHECK, DATE_COLUMN)

print(result_found_next[1])
print(result_found_pre[1])
# 顯示最後一天
# df = pd.read_csv(FILE_PATH, encoding='utf-8-sig')
# print(df.iloc[-1])
