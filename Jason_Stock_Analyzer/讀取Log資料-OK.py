import re
import os
from typing import Optional
import utils.jason_utils as jutils

# 如果這個腳本被直接執行，可以進行簡單測試 (但通常建議透過匯入使用)
if __name__ == '__main__':
    # 請將此路徑替換為您實際的日誌檔路徑進行測試
    test_file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\logs\fetch_summary_20251008.log" 
    # 定義要擷取的前綴
    test_prefix = "[🟢 BWIBBU_d (成功)] 數據筆數:" 
    
    print("--- 測試 log_parser 函式 ---")
    # 呼叫新的函式，傳入兩個參數
    count_string = jutils.get_extracted_string(test_file_path, test_prefix)
    
    if count_string is not None:
        print(f"函式回傳結果 (字串): {count_string}")
        # 如果需要，可以在這裡轉換為整數
        try:
             count_int = int(count_string)
             print(f"轉換為整數: {count_int}")
        except ValueError:
             print("無法將擷取的字串轉換為整數。")
    else:
        print("函式回傳結果: None")
