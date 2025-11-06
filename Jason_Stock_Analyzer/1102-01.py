import pandas as pd
from typing import List, Dict

def get_stock_data_from_excel(file_path: str, sheet_names: List[str]) -> Dict[str, List[str]]:
    """
    讀取指定 Excel 檔案中多個工作表的證券名稱。
    Args:
        file_path (str): Excel 檔案的完整路徑。
        sheet_names (List[str]): 要讀取的活頁簿（工作表）名稱列表。

    Returns:
        Dict[str, List[str]]: 一個字典，鍵為工作表名稱，值為該工作表中的證券名稱列表。
    """
    results = {}
    
    try:
        xls = pd.ExcelFile(file_path)
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到檔案 '{file_path}'")
        return {}
    except Exception as e:
        print(f"❌ 開啟 Excel 檔案時發生未知錯誤: {e}")
        return {}

    # 定義欄位索引
    STOCK_NAME_COL_INDEX = 1      # 假設證券名稱在第 0 欄 (A欄)
    STOCK_COUNT_COL_INDEX = 5     # 假設庫存數量在第 1 欄 (B欄)
    TARGET_SHEET = "股票庫存統計"

    for sheet_name in sheet_names:
        try:
            # 1. 讀取指定的工作表，不帶標題
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

            if sheet_name == TARGET_SHEET:
                # 針對【股票庫存統計】活頁簿：執行篩選
                print(f"🔄 正在對工作表 '{sheet_name}' 執行庫存篩選 (第 {STOCK_COUNT_COL_INDEX} 欄 > 0)...")
                
                # 嘗試將庫存數量欄位轉為數值，非數值者將變為 NaN
                df_count = pd.to_numeric(df.iloc[:, STOCK_COUNT_COL_INDEX], errors='coerce')
                
                # 應用布林篩選：庫存數量欄位的值大於 0
                # 並確保選取範圍不包含 NaN (即非數值或轉換失敗的儲存格)
                filtered_df = df[df_count > 1]
                
                # 擷取篩選後的證券名稱 (第 0 欄)
                stock_names = (
                    filtered_df.iloc[:, STOCK_NAME_COL_INDEX]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
                
            else:
                # 針對其他活頁簿（如【關注的股票】）：僅擷取證券名稱
                stock_names = (
                    df.iloc[:, STOCK_NAME_COL_INDEX]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
            
            # 將結果存入字典
            results[sheet_name] = stock_names
            print(f"✅ 工作表 '{sheet_name}' 最終讀取 {len(stock_names)} 筆證券名稱。")
            
        except ValueError:
            print(f"❌ 警告：工作表 '{sheet_name}' 不存在於檔案中，已跳過。")
            results[sheet_name] = []
        except IndexError:
            # 當工作表沒有足夠的欄位時可能發生
            print(f"❌ 錯誤：工作表 '{sheet_name}' 資料欄位不足 (至少需要 {STOCK_COUNT_COL_INDEX + 1} 欄)。")
            results[sheet_name] = []
        except Exception as e:
            print(f"❌ 讀取工作表 '{sheet_name}' 時發生未知錯誤: {e}")
            results[sheet_name] = []
            
    return results

# --- 範例使用 ---
file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\股票分析.xlsx"
sheets_to_read = ["股票庫存統計", "關注的股票"]

all_stocks_data = get_stock_data_from_excel(file_path, sheets_to_read)

# 輸出結果
print("\n--- 最終結果摘要 ---")

for sheet, stocks in all_stocks_data.items():
    print(f"\n📁 工作表：{sheet}")
    if stocks:
        print(f"總共找到 **{len(stocks)}** 筆證券名稱。")
        print(f"前 3 筆範例: {stocks[:3]}")
    else:
        print("未找到資料或讀取失敗。")