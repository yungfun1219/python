import pandas as pd
import os
from datetime import datetime, timedelta
import re

# 轉換民國年為西元年
def convert_roc_to_gregorian(roc_date_str):
    """將 'YYY/MM/DD' 或 'YYY/M/D' 格式的民國日期字串轉換為 'YYYY/MM/DD' 西元日期字串。"""
    if pd.isna(roc_date_str) or not isinstance(roc_date_str, str):
        return None
    
    parts = roc_date_str.split('/')
    if len(parts) == 3:
        try:
            roc_year = int(parts[0].strip())
            gregorian_year = roc_year + 1911
            
            month = parts[1].strip().zfill(2) 
            day = parts[2].strip().zfill(2)  
            
            return f"{gregorian_year}/{month}/{day}"
        except ValueError:
            return None 
    return None

# --- 設定路徑與證券資訊 ---
STOCK_CODE = "8039"
STOCK_NAME = "台虹" 
input_dir = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\1_STOCK_DAY\\" + STOCK_CODE
output_file = f"{STOCK_CODE}_stocks_data.csv"
output_path = os.path.join(os.path.dirname(input_dir), output_file) 

ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950'] 
PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
VOL_COL = '成交股數' 
ALL_REQUIRED_COLS = ['日期'] + PRICE_COLS + [VOL_COL] 


# --- 資料處理部分 (合併與清理) ---
all_data = []
print(f"--- 🚀 開始處理資料夾：{input_dir} ---")

for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
        filepath = os.path.join(input_dir, filename)
        df, used_encoding = None, None
        
        for encoding in ENCODINGS_TO_TRY:
            try:
                # 讀取時不跳過標頭，確保能獲取正確的欄位名稱
                df = pd.read_csv(filepath, encoding=encoding, header=0) 
                used_encoding = encoding
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"❌ 檔案 {filename} 使用 {encoding} 讀取時發生非編碼錯誤: {e}")
                break
        
        if df is None:
            continue
        
        try:
            df.columns = df.columns.str.strip()
            
            if not all(col in df.columns for col in ALL_REQUIRED_COLS):
                 missing_cols = [col for col in ALL_REQUIRED_COLS if col not in df.columns]
                 raise KeyError(f"檔案欄位名稱不符預期，缺少欄位: {', '.join(missing_cols)}")

            print(f"\nDEBUG: 檔案 {filename} 讀取成功 (使用 {used_encoding} 編碼)。")
            print(f"DEBUG: 清理前資料筆數: {len(df)}")
            
            # --- 步驟 2.1: 處理日期欄位格式 (民國年轉西元年) ---
            date_strings = df['日期'].astype(str).str.strip()
            df['日期'] = [convert_roc_to_gregorian(date_str) for date_str in date_strings]
            
            df.dropna(subset=['日期'], inplace=True)
            df['日期'] = pd.to_datetime(df['日期'], format='%Y/%m/%d')
            
            # --- 步驟 2.2: 清理所有價格和成交量欄位 ---
            all_value_cols = PRICE_COLS + [VOL_COL]
            for col in all_value_cols:
                # 移除逗號
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                # 將所有空值、無效值替換為 NA
                df[col] = df[col].replace(['-', 'nan', 'NaN', ''], pd.NA) 
            
            # 移除包含 NA 的行
            df.dropna(subset=ALL_REQUIRED_COLS, inplace=True)
            
            # 轉換為浮點數（價格）和整數（成交量）
            for col in PRICE_COLS:
                 df[col] = df[col].astype(float)
            df[VOL_COL] = df[VOL_COL].astype(float) # 先轉 float 處理 NA，之後轉 int
            
            all_data.append(df[ALL_REQUIRED_COLS])
            
            print(f"DEBUG: 清理後最終筆數: {len(df)}")
            print(f"✅ 成功處理檔案: {filename} (交易資料 {len(df)} 筆)")
            
        except Exception as e:
            print(f"❌ 檔案 {filename} 處理資料時發生錯誤: {e}")
            continue

# 3. 合併所有資料並進行時間排序
if not all_data:
    print("\n⚠️ 錯誤：資料夾中沒有找到可用的資料檔案。無法進行後續處理。")
    exit()

combined_df = pd.concat(all_data, ignore_index=True)
combined_df.drop_duplicates(subset=['日期'], inplace=True)
combined_df.sort_values(by='日期', ascending=True, inplace=True)
combined_df.reset_index(drop=True, inplace=True)

# 確保成交量為整數
combined_df[VOL_COL] = combined_df[VOL_COL].astype(int) 

print("\n--- 📊 資料合併與排序完成 ---")

# =================================================================
# 4. 計算所有技術指標
# =================================================================

# --- 4.1. MA (MA5, MA10, MA20) ---
combined_df['MA5'] = combined_df['收盤價'].rolling(window=5, min_periods=1).mean().round(2).fillna(0)
combined_df['MA10'] = combined_df['收盤價'].rolling(window=10, min_periods=1).mean().round(2).fillna(0)
combined_df['MA20'] = combined_df['收盤價'].rolling(window=20, min_periods=1).mean().round(2).fillna(0)

# --- 4.2. MACD (12, 26, 9) ---
exp12 = combined_df['收盤價'].ewm(span=12, adjust=False).mean()
exp26 = combined_df['收盤價'].ewm(span=26, adjust=False).mean()
combined_df['MACD'] = (exp12 - exp26).round(2)
combined_df['Signal'] = combined_df['MACD'].ewm(span=9, adjust=False).mean().round(2)
combined_df['MACD_Hist'] = (combined_df['MACD'] - combined_df['Signal']).round(2)

# --- 4.3. KD (9, 3, 3) ---
low_9 = combined_df['最低價'].rolling(window=9).min()
high_9 = combined_df['最高價'].rolling(window=9).max()
combined_df['RSV'] = ((combined_df['收盤價'] - low_9) / (high_9 - low_9) * 100).round(2)
combined_df['K'] = combined_df['RSV'].ewm(com=2, adjust=False).mean().round(2) # com=2 相當於 span=3
combined_df['D'] = combined_df['K'].ewm(com=2, adjust=False).mean().round(2) # com=2 相當於 span=3

# =================================================================
# 【刪除 KDJ (K9, D9, J) 計算】
# --- 4.4. KDJ (K9, D9, J) --- 此部分已刪除
# =================================================================


# --- 4.5. BBands (20, 2) ---
BB_PERIOD = 20
BB_STD_DEV = 2
# 中線 (MB)
combined_df['MB'] = combined_df['收盤價'].rolling(window=BB_PERIOD).mean().round(2)
# 標準差 (Std Dev)
std_dev = combined_df['收盤價'].rolling(window=BB_PERIOD).std()
# 上軌線 (UB)
combined_df['UB'] = (combined_df['MB'] + BB_STD_DEV * std_dev).round(2)
# 下軌線 (LB)
combined_df['LB'] = (combined_df['MB'] - BB_STD_DEV * std_dev).round(2)
# 刪除過渡欄位
combined_df.drop(columns=['MB'], inplace=True)


# --- 4.6. RSI (RSI5, RSI10) ---
def calculate_rsi(data, period):
    # 計算每日漲跌幅
    delta = data.diff()
    # 分離上漲和下跌
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # 計算平滑平均 (通常使用修正的指數移動平均 Wilders Smoothing)
    # pandas ewm(com=period-1) 實現與 RSI 慣用的 Wilders Smoothing 相同
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    
    # 避免除以零
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return rsi.round(2)

combined_df['RSI5'] = calculate_rsi(combined_df['收盤價'], 5)
combined_df['RSI10'] = calculate_rsi(combined_df['收盤價'], 10)


# --- 4.7. VOL MA (VOL5, VOL10) ---
# 原始成交量 (VOL) 就是 '成交股數'，為了方便輸出，將其改名為 VOL
combined_df['VOL'] = combined_df[VOL_COL] 
combined_df['VOL5'] = combined_df['VOL'].rolling(window=5, min_periods=1).mean().round(0).astype(int)
combined_df['VOL10'] = combined_df['VOL'].rolling(window=10, min_periods=1).mean().round(0).astype(int)
# 刪除原始成交股數欄位，只保留 VOL
combined_df.drop(columns=[VOL_COL], inplace=True)


# --- 4.8. 清理所有新增指標的 NaN 值 ---
# 【修正點】：刪除 K9, D9, J
indicator_cols = ['MACD', 'Signal', 'MACD_Hist', 'RSV', 'K', 'D', 'UB', 'LB', 'RSI5', 'RSI10']
combined_df[indicator_cols] = combined_df[indicator_cols].fillna(0)

# 確保 BBands 的 UB/LB 在無效時顯示為 0
combined_df['UB'] = combined_df['UB'].fillna(0)
combined_df['LB'] = combined_df['LB'].fillna(0)


print("--- ✅ 所有指標 (MA, MACD, KD, BBands, RSI, VOL MA) 計算完成 ---")


# 5. 另存檔案為 2330_stocks_data.csv 
combined_df['日期_str'] = combined_df['日期'].dt.strftime('%Y/%m/%d') 

# 【修正點】：刪除 K9, D9, J 輸出欄位
output_cols = (
    ['日期_str'] + PRICE_COLS + 
    ['MA5', 'MA10', 'MA20'] + 
    ['MACD', 'Signal', 'MACD_Hist', 'RSV', 'K', 'D'] + 
    ['UB', 'LB'] + 
    ['RSI5', 'RSI10'] + 
    ['VOL', 'VOL5', 'VOL10'] # K9, D9, J 已刪除
)

combined_df.to_csv(output_path, index=False, encoding='utf-8-sig', columns=output_cols)
combined_df.drop(columns=['日期_str'], inplace=True) 

print(f"\n--- 🎉 資料已處理並儲存至：{output_path} ---")


print(f"\n🎉 總體任務完成！")