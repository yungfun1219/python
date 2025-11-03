import pandas as pd
import re
from typing import Optional

# 讀取 CSV 檔案，篩選出買超的前 20 數據，並只返回證券名稱和買賣超股數欄位。
def get_top_10_institutional_trades_filtered_simplified(
    file_path: str, 
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號",
    name_column: str = "證券名稱" # 假設 '證券名稱' 是您的欄位名
) -> Optional[pd.DataFrame]:
    """
    讀取 CSV 檔案，篩選出買超的前 20 數據，並只返回證券名稱和買賣超股數欄位。
    Args:
        file_path (str): CSV 檔案的路徑。
        volume_column (str): 買賣超股數欄位名稱。
        code_column (str): 證券代號欄位名稱。
        name_column (str): 證券名稱欄位名稱。
    Returns:
        pd.DataFrame or None: 包含 '證券名稱' 和 '買超股數' (單位：張，取整數) 的 DataFrame，失敗則返回 None。
    """
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
    required_cols = [volume_column, code_column, name_column]
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
    df_filtered_code = df[df[code_column].str.match(r'^\d{4}$')]
    
    if df_filtered_code.empty:
        print("ℹ️ 提示：篩選後，沒有找到證券代號為 4 位數字的數據。")
        return pd.DataFrame()

    # 5. 執行篩選條件 2：買賣超股數為正數 (買超)
    df_filtered_positive = df_filtered_code[df_filtered_code[volume_column] > 0]

    if df_filtered_positive.empty:
        print("ℹ️ 提示：篩選後，沒有找到三大法人買超 (正數) 的數據。")
        return pd.DataFrame()

    # 6. 排序並取出前 20 名
    df_sorted = df_filtered_positive.sort_values(
        by=volume_column, 
        ascending=False 
    )
    top_20_trades = df_sorted.head(20) # 取前 20

    # 7. 選擇、單位轉換與格式化輸出
    
    # 選擇需要的原始數值欄位
    result_df = top_20_trades[[name_column, volume_column]].copy()
    
    # 將買超股數除以 1000 並取整數 (向下取整)
    result_df[volume_column] = (result_df[volume_column] / 1000).apply(int) 
    
    # 重新命名欄位以便於顯示和返回 (單位已更改)
    result_df.rename(columns={volume_column: '買超股數(千股)', name_column: '證券名稱'}, inplace=True)

    # --- 控制台輸出顯示 (格式化為字符串，方便閱讀) ---
    display_df = result_df.copy()
    
    # 這裡將千股數加上千分位逗號，用於控制台輸出
    display_df['買超股數(千股)'] = display_df['買超股數(千股)'].apply(lambda x: f"{x:,}")

    col_space_map = {'證券名稱': 15, '買超股數(千股)': 10}
   
    # print("\n✅ 三大法人買超前 20 名 (單位：千股)")
    # print(
    #     display_df.to_string(
    #         index=False, 
    #         col_space=col_space_map, 
    #         justify='left'
    #     )
    # )
    # print("=" * 35)

    # 8. 返回包含數值數據的 DataFrame
    return result_df

# --- 🎯 執行程式 ---

file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\11_T86\20251031_T86_InstitutionalTrades.csv"

# 呼叫函式
top_20_positive_df = get_top_10_institutional_trades_filtered_simplified(file_path)

if top_20_positive_df is not None and not top_20_positive_df.empty:
    # 1. 取得數值欄位 (Series)
    volume_series = top_20_positive_df["買超股數(千股)"]
    # 2. 使用 apply 和 f-string 格式化，將整數轉換為帶逗號的字串
    formatted_volume_series = volume_series.apply(lambda x: f"{x:,}")
    # 3. 合併格式化後的字串和證券名稱
    combined_series = (
        top_20_positive_df["證券名稱"].astype(str).str.strip() + ": " + 
        formatted_volume_series + " (千股)"
    )
    # 輸出合併後的 Series
    print("\n--- 合併後的 Series 範例 (證券名稱:千股,帶逗號) ---")
    print(combined_series)
else:
    print("\n無法生成合併後的 Series，因為返回的 DataFrame 為空或 None。")