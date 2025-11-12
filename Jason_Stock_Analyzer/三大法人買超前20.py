import pandas as pd
import re
from typing import Optional

# 三大法人買超前20
def get_top_10_institutional_trades_filtered(
    file_path: str, 
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號"
) -> Optional[pd.DataFrame]:
    """
    讀取 CSV 檔案，進行以下篩選：
    1. 證券代號必須為 4 位數字。
    2. 三大法人買賣超股數必須為正數 (買超)。
    3. 返回買賣超股數最大的前 10 名數據，並輸出為格式化表格。
    """
    print(f"\n🔄 正在讀取檔案：{file_path}")
    print(f"🎯 篩選條件：1. 代號為 4 位數字 | 2. 買賣超股數 > 0")

    # 1. 讀取 CSV 檔案 (多編碼嘗試)
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except:
                df = pd.read_csv(file_path, encoding='big5')
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的輸入檔案路徑 -> {file_path}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤或編碼問題：{e}")
        return None
    
    # 2. 檢查關鍵欄位是否存在
    required_cols = [volume_column, code_column, '證券名稱']
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"⚠️ 錯誤：檔案中缺少必要的欄位：{missing_cols}。")
        return None

    # 3. 數據清理與數值轉換
    try:
        # 清理買賣超股數欄位：移除引號和逗號
        df[volume_column] = (
            df[volume_column].astype(str).str.replace('"', '', regex=False)
            .str.replace(',', '', regex=False).str.strip()
        )
        # 轉換為數值類型，無法轉換的值設為 NaN
        df[volume_column] = pd.to_numeric(df[volume_column], errors='coerce')
        
        # 清理證券代號欄位
        df[code_column] = df[code_column].astype(str).str.strip()
        
        # 移除無法轉換為數值的行
        df.dropna(subset=[volume_column], inplace=True)
        
    except Exception as e:
        print(f"❌ 數據清理或數值轉換失敗：{e}")
        return None

    # 4. 執行篩選條件 1：證券代號為 4 位數字
    # 使用正則表達式篩選出完全符合四位數字的代號
    df_filtered_code = df[df[code_column].str.match(r'^\d{4}$')]
    
    if df_filtered_code.empty:
        print("ℹ️ 提示：篩選後，沒有找到證券代號為 4 位數字的數據。")
        return pd.DataFrame()

    # 5. 執行篩選條件 2：買賣超股數為正數 (買超)
    df_filtered_positive = df_filtered_code[df_filtered_code[volume_column] > 0]

    if df_filtered_positive.empty:
        print("ℹ️ 提示：篩選後，沒有找到三大法人買超 (正數) 的數據。")
        return pd.DataFrame()

    # 6. 排序並取出前 10 名
    df_sorted = df_filtered_positive.sort_values(
        by=volume_column, 
        ascending=False # 買超數最大的排在最前面
    )
    
    # 取出前 10 筆數據
    top_10_trades = df_sorted.head(20)

    # 7. 輸出結果 (固定欄位寬度與置中對齊)
    
    print(f"\n✅ 篩選後的三大法人買超前 {len(top_10_trades)} 名：")
    print("=" * 40)
    
    # 格式化輸出：將股數轉換為整數格式，並加上千分位逗號
    top_10_trades_display = top_10_trades.copy()
    
    # 重新命名欄位以簡化標題
    volume_col_display_name = '買超股數'
    top_10_trades_display = top_10_trades_display.rename(
        columns={'證券代號': '代號', volume_column: volume_col_display_name}
    )

    # 格式化數字 (加上千分位逗號)
    top_10_trades_display[volume_col_display_name] = top_10_trades_display[volume_col_display_name].apply(lambda x: f"{int(x):,}")

    # 定義輸出的欄位順序
    actual_display_cols = ['代號', '證券名稱', volume_col_display_name]

    # 設定每個欄位的最小寬度，以利置中 (中文字佔 2 寬度)
    col_space_width = 8 

    # 使用 to_string 配合 col_space 和 justify='center'
    print(
        top_10_trades_display[actual_display_cols].to_string(
            index=False,
            col_space=col_space_width, 
            justify='left' # 嘗試置中對齊
        )
    )
    print("=" * 40)
    top_10_trades = top_10_trades_display[actual_display_cols].to_string(
            index=False,
            col_space=col_space_width, 
            justify='left') # 嘗試置中對齊)
    
    return top_10_trades

# --- 🎯 執行程式 ---

# 請將此處的路徑替換為您本地電腦上的實際檔案路徑
file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\11_T86\20251107_T86_InstitutionalTrades.csv"

# 呼叫函式
top_10_positive_df = get_top_10_institutional_trades_filtered(file_path)

print("\n--- 程式執行結束 ---")
print(top_10_positive_df)