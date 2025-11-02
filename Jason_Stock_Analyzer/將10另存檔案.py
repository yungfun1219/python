import pandas as pd
import numpy as np

def analyze_stock_data_clean(file_path, target_name):
    """
    讀取指定的CSV檔案，先刪除所有數據皆為空白的欄位，
    然後篩選出特定證券名稱的買賣超股數數據。
    Args:
        file_path (str): CSV檔案的完整路徑。
        target_name (str): 要篩選的證券名稱 (預設為 '台玻')。
    Returns:
        pd.DataFrame or None: 包含目標證券數據的DataFrame，如果檔案不存在則返回 None。
    """
    try:
        # 1. 讀取CSV檔案
        # 嘗試常見的編碼，如果遇到問題，請手動調整 encoding
        df = pd.read_csv(file_path, encoding='utf-8')
        # 備註：如果遇到編碼問題，請嘗試：df = pd.read_csv(file_path, encoding='big5')

        initial_columns_count = len(df.columns)
        
        # 2. 【核心步驟】刪除空白欄位 (即所有值都是 NaN 的欄位)
        # axis=1 代表操作欄位；how='all' 代表只有當該欄位所有值都為 NaN 時才刪除
        df_cleaned = df.dropna(axis=1, how='all')

        removed_columns_count = initial_columns_count - len(df_cleaned.columns)
        if removed_columns_count > 0:
             print(f"✅ 成功刪除 {removed_columns_count} 個空白欄位。")
        else:
             print("✅ 沒有偵測到完全空白的欄位需要刪除。")


        # 3. 檢查必要的欄位是否存在
        required_columns = ['證券名稱', '買賣超股數']
        # 注意：如果原始檔案中的欄位名與此處不符，程式將會報錯。
        if not all(col in df_cleaned.columns for col in required_columns):
            print(f"⚠️ 錯誤：清理後的檔案中缺少必要的欄位。所需欄位：{required_columns}")
            print(f"清理後的檔案欄位：{list(df_cleaned.columns)}")
            return None

        # 4. 篩選出目標證券名稱的數據
        target_data = df_cleaned[df_cleaned['證券名稱'] == target_name]

        # 5. 返回結果
        if target_data.empty:
            print(f"ℹ️ 提示：在檔案中找不到證券名稱為 '{target_name}' 的數據。")
            return target_data[['證券名稱', '買賣超股數']] if '證券名稱' in df_cleaned.columns and '買賣超股數' in df_cleaned.columns else pd.DataFrame()
        else:
            final_data = target_data[['證券名稱', '買賣超股數']]
            print(f"\n✅ 成功找到 '{target_name}' 的買賣超股數數據：")
            print("--- 數據結果 ---")
            print(final_data)
            print("----------------")
            return final_data


    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的檔案路徑 -> {file_path}")
        return None
    except pd.errors.EmptyDataError:
        print(f"❌ 錯誤：檔案是空的或無效的數據格式 -> {file_path}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤：{e}")
        return None

# --- 🎯 執行範例 ---
file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\output\20251102_TWT44U_SelectedColumns_Fixed.csv"
stock_name = "台玻" # 您要查詢的證券名稱

# 呼叫函式
result_data = analyze_stock_data_clean(file_path, stock_name)

# 如果找到數據，可以進一步計算總和
if result_data is not None and not result_data.empty:
    # 確保 '買賣超股數' 是數值型態，以防萬一
    try:
        total_net_buy = result_data['買賣超股數']
        print(f"\n✨ '{stock_name}' 總買賣超股數：{int(total_net_buy)} 股")
    except KeyError:
        print("❌ 錯誤：無法計算總和，因為 '買賣超股數' 欄位不存在或格式不正確。")
    except ValueError:
        print("❌ 錯誤：'買賣超股數' 欄位包含非數值資料，無法計算總和。")

print("\n--- 程式結束 ---")
print(result_data)