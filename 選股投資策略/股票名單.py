import pandas as pd
import os

def create_stock_dict_from_csv(file_path):
    """
    從 CSV 檔案中讀取「有價證券代號」和「有價證券名稱」欄位，並轉換成字典。

    Args:
        file_path (str): CSV 檔案的完整路徑。

    Returns:
        dict: 包含股票代號和名稱的字典，如果檔案不存在則返回 None。
    """
    # 檢查檔案路徑是否存在
    if not os.path.exists(file_path):
        print(f"錯誤：檔案路徑不存在 -> {file_path}")
        return None

    try:
        # 1. 使用 pandas 讀取 CSV 檔案
        # 假設 CSV 檔案可能包含中文，使用 'utf-8' 或 'big5' 編碼嘗試讀取
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='big5')
        
        # 2. 確保所需的欄位名稱存在
        code_col = '有價證券代號'
        name_col = '有價證券名稱'
        #name_col = '有價證券名稱'
        
        if code_col not in df.columns or name_col not in df.columns:
            print("錯誤：CSV 檔案中缺少必要的欄位名稱（'有價證券代號' 或 '有價證券名稱'）。")
            print(f"檔案中的實際欄位名稱：{list(df.columns)}")
            return None
        
        # 3. 處理缺失值 (NA/NaN) 或空字串
        # 移除代號或名稱為空的列，並確保代號為字串格式
        df = df.dropna(subset=[code_col, name_col])
        df[code_col] = df[code_col].astype(str).str.strip()
        df[name_col] = df[name_col].astype(str).str.strip()

        # 4. 使用 pandas 的 to_dict() 方法將這兩欄轉換為字典
        # 'series' 參數會創建一個 {代號: 名稱} 的字典
        stock_dict = df.set_index(code_col)[name_col].to_dict()

        print("股票代號與名稱字典建立成功！")
        return stock_dict

    except Exception as e:
        print(f"讀取或轉換檔案時發生錯誤：{e}")
        return None

# --- 執行部分 ---
# 設定您的檔案路徑
file_path = r'D:\Python_repo\python\選股投資策略\exchange_list.csv'

# 呼叫函數並取得字典
symbol_dict = create_stock_dict_from_csv(file_path)

# 顯示結果 (只顯示前 5 筆，以避免輸出過長)
if symbol_dict:
    print("\n--- 產生的字典 (前 5 筆) ---")
    # 由於字典沒有順序，我們將其轉換為列表後取前 5 筆
    first_5_items = list(symbol_dict.items())[:5]
    for code, name in first_5_items:
        print(f"'{code}': '{name}',")
    print("...")