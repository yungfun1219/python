import utils.jason_utils as jutils

# 檢查檔案是否存在且確實是一個檔案 (非資料夾)
def check_folder_and_create(folder_path: str):
    """
    參數:
        file_path (str): 要檢查的檔案路徑。
    回傳:
        bool: 檔案存在時回傳 True；否則回傳 False。
    """
    OUTPUT_DIR, filename_new = jutils.get_path_to_folder_file(folder_path)
    jutils.check_and_create_folder(OUTPUT_DIR)
    jutils.check_file_exists(filename_new)
    return True


# --- 額外功能: 計算合併後檔案中的股票數量 (維持不變) ---
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



