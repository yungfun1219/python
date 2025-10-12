import pandas as pd
import os
from typing import Optional

def count_stocks_in_csv(file_path: str) -> Optional[int]:
    """
    讀取指定的 CSV 檔案，並計算其中的資料列數（即股票數量）。
    Args:
        file_path: 欲讀取的 CSV 檔案的完整路徑 (str)。
    Returns:
        成功時回傳股票數量 (int)；若檔案不存在或讀取失敗則回傳 None。
    """
    
    # 1. 檢查檔案是否存在
    if not os.path.exists(file_path):
        print(f"錯誤：檔案不存在於 {file_path}")
        return None

    # 2. 嘗試讀取 CSV 檔案
    # 設定編碼優先順序：'utf-8' -> 'big5'
    encodings_to_try = ['utf-8', 'big5']
    df = None
    
    for encoding in encodings_to_try:
        try:
            # header=0 告訴 Pandas 檔案的第一行是欄位標題
            df = pd.read_csv(file_path, header=0, encoding=encoding)
            # 如果成功讀取，則跳出迴圈
            # print(f"成功使用 {encoding} 編碼讀取檔案。")
            break
        except Exception:
            # 忽略當前的編碼錯誤，嘗試下一個編碼
            continue
    
    # 3. 檢查是否成功讀取
    if df is None:
        print(f"錯誤：嘗試了 {', '.join(encodings_to_try)} 編碼，但讀取檔案失敗。")
        return None
    
    # 4. 計算股票數量並回傳
    # df.shape[0] 即為資料列數 (行數)
    stock_count = df.shape[0]
    return stock_count

# --- 範例使用方式 ---

# 定義您的檔案路徑
csv_file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\stocks_all.csv"

# 呼叫函式
total_stocks = count_stocks_in_csv(csv_file_path)

# 處理回傳結果
if total_stocks is not None:
    print("-" * 30)
    print(f"檔案 '{os.path.basename(csv_file_path)}' 中的股票總數量為：{total_stocks} 支")
    print("-" * 30)
else:
    print("無法取得股票數量。")