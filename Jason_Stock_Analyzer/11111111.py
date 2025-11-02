import pandas as pd
import re
from typing import Optional

def get_top_10_institutional_trades_filtered(
    file_path: str, 
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號"
) -> Optional[pd.DataFrame]:
    """
    讀取 CSV 檔案，進行以下篩選：
    1. 證券代號必須為 4 位數字。
    2. 三大法人買賣超股數必須為正數 (買超)。
    3. 返回買賣超股數最大的前 10 名數據。
    
    Args:
        file_path (str): CSV 檔案的路徑。
        volume_column (str): 買賣超股數欄位名稱。
        code_column (str): 證券代號欄位名稱。
        
    Returns:
        pd.DataFrame or None: 包含篩選後前 10 名數據的 DataFrame，失敗則返回 None。
    """
    print(f"\n🔄 正在讀取檔案：{file_path}")
    print(f"🎯 篩選條件：1. 代號為 4 位數字 | 2. 買賣超股數 > 0")

    # 1. 讀取 CSV 檔案 (沿用多編碼嘗試)
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
    required_cols = [volume_column, code_column]
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"⚠️ 錯誤：檔案中缺少必要的欄位：{missing_cols}。")
        return None

    # 3. 數據清理與數值轉換
    try:
        # 清理買賣超股數欄位
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
    # ^: 字串開頭, \d{4}: 剛好四位數字, $: 字串結尾
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
    
    # 由於數據已經是正數，直接按降序排序即可 (不需要使用絕對值)
    df_sorted = df_filtered_positive.sort_values(
        by=volume_column, 
        ascending=False # 買超數最大的排在最前面
    )
    
    # 取出前 10 筆數據
    top_10_trades = df_sorted.head(20)

    # 7. 輸出結果 (固定欄位寬度)
    
    # 格式化輸出：將股數轉換為整數格式，並加上千分位逗號
    top_10_trades_display = top_10_trades.copy()
    top_10_trades_display[volume_column] = top_10_trades_display[volume_column].apply(lambda x: f"{int(x):,}")

    # 定義輸出的欄位順序
    display_cols = [code_column, '證券名稱', volume_column]
    actual_display_cols = [col for col in display_cols if col in top_10_trades_display.columns]
    
    # 調整欄位名稱，讓它更適合顯示
    top_10_trades_display = top_10_trades_display.rename(columns={volume_column: '買超股數'})
    #actual_display_cols = [code_column, '證券名稱', '買超股數']
    actual_display_cols = ['證券名稱', '買超股數']

    # ⭐ 核心修改點：使用 to_string 配合 col_space 參數
    
    # 設定每個欄位的最小寬度 (可以根據實際數據長度調整)
    # 例如：代號: 8, 名稱: 10, 股數: 15
    # 如果您需要更精準的控制，可以使用字典：
    # col_space_map = {'證券代號': 8, '證券名稱': 10, '買超股數': 15}
    
    # 我們先使用一個單一整數來固定所有欄位的寬度 (例如：18)
    col_space_width = 8 

    print(
        top_10_trades_display[actual_display_cols].to_string(
            index=False,      # 不顯示索引
            col_space=col_space_width, # 設定欄位寬度
            justify='left'    # 讓數據靠左對齊 (可選)
        )
    )
    print("=" * 40)
    
    top_10_trades = top_10_trades_display[actual_display_cols].to_string(
            index=False,      # 不顯示索引
            col_space=col_space_width, # 設定欄位寬度
            justify='right'    # 讓數據靠右對齊 (可選)
        )
    return top_10_trades

# --- 🎯 執行程式 ---

file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\11_T86\20251030_T86_InstitutionalTrades.csv"

# 呼叫函式
top_10_positive_df = get_top_10_institutional_trades_filtered(file_path)

print("\n--- 程式執行結束 ---")

#df_str = top_10_positive_df.astype(str)
print(top_10_positive_df)