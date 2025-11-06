import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional

    
# 從指定的 Excel 檔案中讀取兩個工作表：
def get_stock_names_from_excel_sheets(
    excel_file_path: str = Path(__file__).resolve().parent / "datas" / "股票分析.xlsx",
    stock_inventory_sheet: str = "股票庫存統計",
    watch_list_sheet: str = "關注的股票",
    inventory_filter_column: str = "目前股數庫存統計",
    filter_value: any = 0
) -> Tuple[List[str], List[str]]:
    """
    1. 【股票庫存統計】：篩選 '目前股數庫存統計' 不等於 0 的證券名稱。
    2. 【關注的股票】：讀取所有證券名稱。
    Args:
        excel_file_path (str): 原始 Excel 檔案的完整路徑。
        stock_inventory_sheet (str): 股票庫存統計的工作表名稱。
        watch_list_sheet (str): 關注的股票的工作表名稱。
        inventory_filter_column (str): 股票庫存統計中用於篩選的欄位名稱。
        filter_value (any): 要篩除的值 (預設為 0)。
    Returns:
        Tuple[List[str], List[str]]: (庫存統計後的證券名稱列表, 關注的股票證券名稱列表)。
                                     如果讀取失敗，對應列表為空。
    """
    original_path = Path(excel_file_path)
    
    if not original_path.exists():
        print(f"❌ 錯誤：找不到 Excel 檔案在路徑：{excel_file_path}")
        return [], []

    # 初始化結果列表
    inventory_stocks: List[str] = []
    watchlist_stocks: List[str] = []

    # 假設證券名稱欄位名，需要根據您實際的 Excel 結構調整
    STOCK_NAME_COLUMN = "證券名稱" 

    print(f"✅ 正在讀取 Excel 檔案：{original_path.name}\n")

    # --- 子函式：處理單一工作表的讀取和篩選邏輯 ---
    def process_sheet(sheet_name: str, apply_filter: bool) -> List[str]:
        """讀取並可選地篩選單一工作表，回傳證券名稱列表。"""
        print(f"🎯 處理工作表：{sheet_name}")
        
        try:
            # 讀取 Excel 中指定工作表的資料
            # 沿用您原始程式碼的讀取參數設定
            df = pd.read_excel(
                original_path, 
                sheet_name=sheet_name, 
                header=0,
                skiprows=[1]  # 跳過 Excel 中的第二行
            )
            
            if df.empty:
                print(f"警告：工作表 '{sheet_name}' 讀取到的數據為空。")
                return []
            
            # 檢查證券名稱欄位是否存在
            if STOCK_NAME_COLUMN not in df.columns:
                 print(f"❌ 錯誤：工作表 '{sheet_name}' 中找不到欄位 '{STOCK_NAME_COLUMN}'。")
                 print(f"  -> 找到的欄位有: {list(df.columns)}")
                 return []

            if apply_filter:
                # 【股票庫存統計】的篩選邏輯
                if inventory_filter_column not in df.columns:
                    print(f"❌ 錯誤：找不到篩選欄位 '{inventory_filter_column}'。跳過篩選。")
                else:
                    initial_rows = len(df)
                    print(f"🔍 篩選中：保留 '{inventory_filter_column}' != {filter_value} 的資料...")
                    
                    # 轉換為數值，非數值則為 NaN
                    df[inventory_filter_column] = pd.to_numeric(
                        df[inventory_filter_column], errors='coerce'
                    )
                    
                    # 執行篩選：保留數值欄位中不等於 filter_value 的行
                    df = df[df[inventory_filter_column] != float(filter_value)]
                    
                    removed_rows = initial_rows - len(df)
                    print(f"  -> 篩選後剩餘筆數: {len(df)} 筆 (移除 {removed_rows} 筆)")

            # 提取證券名稱列表
            # .dropna() 移除空值； .astype(str) 確保為字串； .tolist() 轉換為列表
            stock_names = df[STOCK_NAME_COLUMN].dropna().astype(str).tolist()
            print(f"  -> 成功取得 {len(stock_names)} 筆證券名稱。\n")
            return stock_names

        except ValueError as e:
            if "Worksheet named" in str(e):
                print(f"❌ 錯誤：找不到工作表 '{sheet_name}'。")
            else:
                print(f"❌ 讀取工作表 '{sheet_name}' 時發生錯誤：{e}")
            return []
        except Exception as e:
            print(f"❌ 處理工作表 '{sheet_name}' 時發生未知錯誤：{e}")
            return []

    # 1. 處理【股票庫存統計】 (需篩選)
    inventory_stocks = process_sheet(stock_inventory_sheet, apply_filter=True)

    # 2. 處理【關注的股票】 (不篩選)
    watchlist_stocks = process_sheet(watch_list_sheet, apply_filter=False)

    return inventory_stocks, watchlist_stocks

#--- 主程式區塊：執行函式並顯示結果 ---
try:
    # 執行函式並接收兩個列表
    inventory_list, watchlist_list = get_stock_names_from_excel_sheets()       # 這裡可以省略後面的參數，如果工作表名稱和篩選欄位名與預設值相同
    

    print("--- 🌟 最終結果 ---")
    
    # 庫存統計結果
    print(f"📦 【股票庫存統計】 (股數 > 0) 列表 (共 {len(inventory_list)} 筆):")
    # print(inventory_list[:5], "..." if len(inventory_list) > 5 else "")
    
    # # 關注的股票結果
    # print(f"\n👀 【關注的股票】 列表 (共 {len(watchlist_list)} 筆):")
    # print(watchlist_list[:5], "..." if len(watchlist_list) > 5 else "")
    for stock in inventory_list:
        print(stock)


    print(f"\n👀 【關注的股票】 列表 (共 {len(watchlist_list)} 筆):")
    for stock in watchlist_list:
        print(stock)
        
except Exception as e:
    print(f"程式運行期間發生異常：{e}")