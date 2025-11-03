import pandas as pd
import numpy as np # 導入 numpy 以便進行數值操作

# 讀取 CSV 檔案，篩選出指定證券名稱的資料，並只返回「買賣超股數」數據。
def get_stock_net_volume(file_path, target_name, target_column="三大法人買賣超股數"):
    """
    讀取 CSV 檔案，篩選出指定證券名稱的資料，並只返回「買賣超股數」數據。
    Args:
        file_path (str): CSV檔案的完整路徑。
        target_name (str): 要篩選的證券名稱。
        target_column (str): 要取出的欄位名稱 (預設為 '買賣超股數')。
    Returns:
        pd.Series or None: 包含目標買賣超股數的 Series，如果讀取或篩選失敗則返回 None。
    """
    print(f"🔄 正在讀取檔案：{file_path}")
    print(f"🎯 搜尋目標：【{target_name}】，並取出【{target_column}】數據")

    # 1. 讀取CSV檔案 (多編碼嘗試，確保輸入正確)
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            print("ℹ️ 成功使用 'utf-8-sig' 編碼讀取。")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                print("ℹ️ 使用 'utf-8' 編碼讀取。")
            except:
                df = pd.read_csv(file_path, encoding='big5')
                print("ℹ️ 使用 'big5' 編碼讀取。")
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的輸入檔案路徑 -> {file_path}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤或編碼問題：{e}")
        return None
    
    # 2. 檢查關鍵欄位是否存在
    required_cols = ['證券名稱', target_column]
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"⚠️ 錯誤：檔案中缺少必要的欄位：{missing_cols}。")
        print(f"檔案實際欄位名稱：{list(df.columns)}")
        return None

    # 3. 數據清理與篩選
    # 清理 '證券名稱' 兩側空白，確保精確匹配
    df['證券名稱'] = df['證券名稱'].astype(str).str.strip()

    # ⭐ 核心修改點 A: 清理 '買賣超股數' 欄位，移除引號並清理空白，為數值轉換做準備
    try:
        df[target_column] = df[target_column].astype(str).str.replace('"', '', regex=False).str.strip()
        # print(f"✅ 成功移除 {target_column} 欄位中的雙引號。")
    except Exception as e:
        print(f"⚠️ 警告：嘗試清理 {target_column} 時發生錯誤：{e}")

    target_data = df[df['證券名稱'] == target_name]

    # 4. 取出目標欄位數據
    if target_data.empty:
        print(f"\nℹ️ 提示：在檔案中找不到證券名稱為 【{target_name}】 的數據。")
        return pd.Series(dtype='object')
    else:
        # 取出 '買賣超股數' 欄位，這是一個 pandas.Series 對象
        net_volume_series = target_data[target_column]
        
        print(f"\n✅ 成功找到 【{target_name}】 的 {len(net_volume_series)} 筆【{target_column}】數據。")
        print("-" * 60)
        # 這裡不顯示 Series 原始內容，讓最終輸出更聚焦
        
    return net_volume_series

# --- 🎯 執行程式 ---
file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\11_T86\20251031_T86_InstitutionalTrades.csv"
stock_name = "金像電" # 目標證券名稱

# 呼叫函式
net_volume_data = get_stock_net_volume(file_path, stock_name)

# ----------------------------------------------------
# --- 最終處理：轉換為「張」並列印 ---
# ----------------------------------------------------

print("\n--- 程式執行結束 ---\n")

if net_volume_data is not None and not net_volume_data.empty:
    try:
        # 1. 轉換為數值 (float)，並除以 1000 換算成「張」
        net_volume_in_lots = net_volume_data.astype(float) / 1000
        
        # 2. (可選) 對結果進行四捨五入或取整數
        # 這裡使用 round() 保持一定精確度，您可以根據需求改為 .astype(int)
        rounded_lots = net_volume_in_lots.round(0).astype(int) 
        
        # 3. 將 Series 轉換為字串 (不含索引，且不含標題)
        # 使用 to_string(index=False, header=False) 取得純數據字串
        output_string = rounded_lots.to_string(index=False, header=False).strip()
        
        # 4. 替換 to_string 產生的多餘換行或空格，並加入單位
        # 由於 to_string 會產生多行，這裡用換行符 \n 格式化輸出
        
        print(f"【{stock_name}】買賣超股數 (單位：張)：")
        print("-------------------------------")
        # 直接輸出轉換後的 Series，確保索引或行數信息不會丟失
        # 為了美觀，我們輸出 Series，並在旁邊加上單位
        
        # 使用 apply 來確保每行都加上 "張"
        formatted_series = rounded_lots.astype(str).apply(lambda x: f"{x} 張")
        print(formatted_series)
        print("-------------------------------")
        
    except ValueError as e:
        print(f"❌ 錯誤：數據中包含無法轉換為數值的資料，無法換算成「張」。")
        # print(f"  詳細錯誤：{e}") # 方便除錯
        
else:
    print(f"找不到 {stock_name} 的買賣超股數資料或資料為空。")