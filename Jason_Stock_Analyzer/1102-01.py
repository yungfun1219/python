import pandas as pd
from typing import Optional

def get_stock_names_from_excel(file_path: str, sheet_name: str, column_name: str) -> Optional[pd.Series]:
    """
    讀取 Excel 檔案中指定工作表的指定欄位數據。

    Args:
        file_path (str): Excel 檔案的完整路徑。
        sheet_name (str): 工作表的標籤名稱 (e.g., '【關注的股票】')。
        column_name (str): 要抓取的欄位名稱 (e.g., '證券名稱')。

    Returns:
        pd.Series or None: 包含證券名稱的 Series，如果失敗則返回 None。
    """
    print(f"🔄 正在嘗試讀取 Excel 檔案：{file_path}")
    print(f"🎯 鎖定工作表：【{sheet_name}】")

    try:
        # 讀取 Excel 檔案中指定的工作表
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # 檢查欄位是否存在
        if column_name in df.columns:
            # 抓取並返回 '證券名稱' 欄位的資料
            stock_names = df[column_name]
            
            print(f"✅ 成功抓取工作表 '{sheet_name}' 中 '{column_name}' 欄位的數據。")
            
            # 輸出列表內容
            print("-" * 50)
            print("【證券名稱】列表：")
            print(stock_names.to_string(index=False)) # 輸出乾淨的列表
            print("-" * 50)
            
            return stock_names
        else:
            print(f"❌ 錯誤：工作表 '{sheet_name}' 中找不到欄位 '{column_name}'。")
            print(f"實際欄位名稱：{list(df.columns)}")
            return None

    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的 Excel 檔案路徑 -> {file_path}")
        return None
    except ValueError as e:
        if "Worksheet named" in str(e):
            print(f"❌ 錯誤：找不到名為 '{sheet_name}' 的工作表。請檢查標籤名稱是否正確。")
        else:
            print(f"❌ 讀取 Excel 檔案時發生錯誤: {e}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        return None

# --- 🎯 執行程式 ---

file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\股票分析.xlsx"
focused_sheet_name = "關注的股票"
focused_column_name = "證券名稱"

# 呼叫函式
focused_stocks = get_stock_names_from_excel(file_path, focused_sheet_name, focused_column_name)