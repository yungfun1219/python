import pandas as pd
import os
import tkinter as tk
from tkinter import messagebox
from datetime import datetime, timedelta
import re
import csv
import sys # 用於安全地退出程式

# =================================================================
# 數據處理和計算部分 (原 main_run 程式)
# =================================================================

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

def calculate_rsi(data, period):
    """計算 RSI 指標"""
    delta = data.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # 使用 Wilders Smoothing (ewm(com=period-1))
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    
    # 避免除以零
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return rsi.round(2)


def stock_data_processor(target_date_str):
    """
    執行股票數據的讀取、清理、指標計算和儲存。
    
    Args:
        target_date_str (str): 使用者輸入的日期 (格式如 20251128)，
                                雖然暫未用於邏輯篩選，但作為參數傳入。
                                
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        # --- 設定路徑與證券資訊 ---
        STOCK_CODE = "8039"
        STOCK_NAME = "台虹" 
        # ⚠️ 這裡請確保您的路徑存在，否則會拋出錯誤
        input_dir = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\1_STOCK_DAY\\" + STOCK_CODE
        output_file = f"{STOCK_CODE}_stocks_data.csv"
        # 假設輸出檔案位於 input_dir 的上層目錄
        output_path = os.path.join(os.path.dirname(input_dir), output_file) 

        ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950'] 
        PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
        VOL_COL = '成交股數' 
        ALL_REQUIRED_COLS = ['日期'] + PRICE_COLS + [VOL_COL] 

        # --- 資料處理部分 (合併與清理) ---
        all_data = []
        if not os.path.isdir(input_dir):
            return False, f"❌ 資料夾不存在: {input_dir}"
            
        print(f"--- 🚀 開始處理資料夾：{input_dir} (目標日期: {target_date_str}) ---")

        for filename in os.listdir(input_dir):
            if filename.endswith(".csv"):
                # ... (檔案讀取和清理邏輯，與您原來的程式碼相同，略過中間輸出以保持簡潔)
                filepath = os.path.join(input_dir, filename)
                df, used_encoding = None, None
                
                for encoding in ENCODINGS_TO_TRY:
                    try:
                        df = pd.read_csv(filepath, encoding=encoding, header=0) 
                        used_encoding = encoding
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        print(f"❌ 檔案 {filename} 讀取時發生錯誤: {e}")
                        break
                
                if df is None:
                    continue
                
                try:
                    df.columns = df.columns.str.strip()
                    if not all(col in df.columns for col in ALL_REQUIRED_COLS):
                         missing_cols = [col for col in ALL_REQUIRED_COLS if col not in df.columns]
                         raise KeyError(f"檔案欄位名稱不符預期，缺少欄位: {', '.join(missing_cols)}")

                    date_strings = df['日期'].astype(str).str.strip()
                    df['日期'] = [convert_roc_to_gregorian(date_str) for date_str in date_strings]
                    df.dropna(subset=['日期'], inplace=True)
                    df['日期'] = pd.to_datetime(df['日期'], format='%Y/%m/%d')
                    
                    all_value_cols = PRICE_COLS + [VOL_COL]
                    for col in all_value_cols:
                        df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                        df[col] = df[col].replace(['-', 'nan', 'NaN', ''], pd.NA) 
                    
                    df.dropna(subset=ALL_REQUIRED_COLS, inplace=True)
                    
                    for col in PRICE_COLS:
                         df[col] = df[col].astype(float)
                    df[VOL_COL] = df[VOL_COL].astype(float) 
                    
                    all_data.append(df[ALL_REQUIRED_COLS])
                    
                except Exception as e:
                    print(f"❌ 檔案 {filename} 處理資料時發生錯誤: {e}")
                    continue

        if not all_data:
            return False, "⚠️ 錯誤：資料夾中沒有找到可用的資料檔案。無法進行後續處理。"

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.drop_duplicates(subset=['日期'], inplace=True)
        combined_df.sort_values(by='日期', ascending=True, inplace=True)
        combined_df.reset_index(drop=True, inplace=True)
        combined_df[VOL_COL] = combined_df[VOL_COL].astype(int) 

        # =================================================================
        # 4. 計算所有技術指標 (與您調整後的程式碼相同)
        # =================================================================

        # --- 4.1. MA ---
        combined_df['MA5'] = combined_df['收盤價'].rolling(window=5, min_periods=1).mean().round(2).fillna(0)
        combined_df['MA10'] = combined_df['收盤價'].rolling(window=10, min_periods=1).mean().round(2).fillna(0)
        combined_df['MA20'] = combined_df['收盤價'].rolling(window=20, min_periods=1).mean().round(2).fillna(0)

        # --- 4.2. MACD ---
        exp12 = combined_df['收盤價'].ewm(span=12, adjust=False).mean()
        exp26 = combined_df['收盤價'].ewm(span=26, adjust=False).mean()
        combined_df['MACD'] = (exp12 - exp26).round(2)
        combined_df['Signal'] = combined_df['MACD'].ewm(span=9, adjust=False).mean().round(2)
        combined_df['MACD_Hist'] = (combined_df['MACD'] - combined_df['Signal']).round(2)

        # --- 4.3. KD ---
        low_9 = combined_df['最低價'].rolling(window=9).min()
        high_9 = combined_df['最高價'].rolling(window=9).max()
        combined_df['RSV'] = ((combined_df['收盤價'] - low_9) / (high_9 - low_9) * 100).round(2)
        combined_df['K'] = combined_df['RSV'].ewm(com=2, adjust=False).mean().round(2)
        combined_df['D'] = combined_df['K'].ewm(com=2, adjust=False).mean().round(2)

        # --- 4.5. BBands (20, 2) ---
        BB_PERIOD = 20
        BB_STD_DEV = 2

        # 1. 計算中線 (MB) - 20日 SMA
        combined_df['MB'] = combined_df['收盤價'].rolling(window=BB_PERIOD).mean().round(2)

        # 2. 計算標準差 (Std Dev) - 20日
        combined_df['STD'] = combined_df['收盤價'].rolling(window=BB_PERIOD).std()

        # 3. 計算上軌線 (UB)
        # 使用 .copy() 避免 SettingWithCopyWarning，並明確指定 round(2)
        combined_df['UB'] = (combined_df['MB'] + BB_STD_DEV * combined_df['STD']).round(2).copy() 

        # 4. 計算下軌線 (LB)
        combined_df['LB'] = (combined_df['MB'] - BB_STD_DEV * combined_df['STD']).round(2).copy()

        # 5. 刪除過渡欄位
        combined_df.drop(columns=['MB', 'STD'], inplace=True)

        # --- 4.6. RSI (RSI5, RSI10) ---
        combined_df['RSI5'] = calculate_rsi(combined_df['收盤價'], 5)
        combined_df['RSI10'] = calculate_rsi(combined_df['收盤價'], 10)

        # --- 4.7. VOL MA (VOL5, VOL10) ---
        combined_df['VOL'] = combined_df[VOL_COL] 
        combined_df['VOL5'] = combined_df['VOL'].rolling(window=5, min_periods=1).mean().round(0).astype(int)
        combined_df['VOL10'] = combined_df['VOL'].rolling(window=10, min_periods=1).mean().round(0).astype(int)
        combined_df.drop(columns=[VOL_COL], inplace=True)

        # --- 4.8. 清理所有新增指標的 NaN 值 ---
        indicator_cols = ['MACD', 'Signal', 'MACD_Hist', 'RSV', 'K', 'D', 'UB', 'LB', 'RSI5', 'RSI10']
        combined_df[indicator_cols] = combined_df[indicator_cols].fillna(0)
        combined_df['UB'] = combined_df['UB'].fillna(0)
        combined_df['LB'] = combined_df['LB'].fillna(0)


        # 5. 另存檔案為 2330_stocks_data.csv 
        combined_df['日期_str'] = combined_df['日期'].dt.strftime('%Y/%m/%d') 
        output_cols = (
            ['日期_str'] + PRICE_COLS + 
            ['MA5', 'MA10', 'MA20'] + 
            ['MACD', 'Signal', 'MACD_Hist', 'RSV', 'K', 'D'] + 
            ['UB', 'LB'] + 
            ['RSI5', 'RSI10'] + 
            ['VOL', 'VOL5', 'VOL10']
        )
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig', columns=output_cols)
        
        return True, f"數據處理成功！\n\n已計算指標並儲存至：\n{output_path}\n(目標日期: {target_date_str})"

    except Exception as e:
        # 捕捉在數據處理和計算過程中發生的任何錯誤
        return False, f"數據處理失敗！\n\n錯誤類型: {type(e).__name__}\n錯誤訊息: {e}"


# =================================================================
# Tkinter UI 部分
# =================================================================

def run_full_process(root, entry_widget):
    """
    獲取輸入內容，執行數據處理，然後關閉視窗。
    """
    # 1. 取得使用者在 Entry 輸入的內容
    user_data = entry_widget.get()
    
    # 2. 關閉 UI 視窗 (必須先關閉，才能繼續執行主邏輯)
    root.destroy()
    
    # 3. 執行數據處理邏輯
    # 由於我們不能在 close_and_save 中使用全域變數，我們將讓這個函式回傳結果。
    # 為了模擬這個過程，我們將結果存在一個獨立的變數中，然後在主程式的最後處理。
    
    # 執行處理器，並獲取結果
    success, message = stock_data_processor(user_data)
    
    # 4. 顯示最終結果提示小視窗
    if success:
        messagebox.showinfo(
            "程式執行完畢 - 成功", 
            f"✅ 程式執行完畢！\n\n{message}"
        )
    else:
        messagebox.showerror(
            "程式執行完畢 - 失敗", 
            f"❌ 程式執行失敗！\n\n{message}"
        )
        
    # 程式流程結束
    sys.exit() # 確保所有視窗都關閉


def display_main_window_with_input(title, fixed_message, input_label_text):
    """
    建立帶有訊息、輸入框和儲存按鈕的主視窗，並讓它居中。
    """
    root = tk.Tk()
    root.title(title)
    
    # 設置視窗的固定尺寸
    window_width = 500
    window_height = 300
    
    # 計算螢幕中央座標
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    
    center_x = int((screen_width / 2) - (window_width / 2))
    center_y = int((screen_height / 2) - (window_height / 2))
    
    # 設置視窗大小和位置 (格式: "寬x高+X+Y")
    root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    
    # --- 1. 固定訊息標籤 ---
    message_label = tk.Label(
        root, 
        text=fixed_message, 
        font=("Arial", 12), 
        wraplength=450,
        justify=tk.LEFT
    )
    message_label.pack(padx=20, pady=(20, 10), anchor='w') # anchor='w' 靠左對齊

    # --- 2. 輸入框提示標籤 ---
    input_label = tk.Label(
        root, 
        text=input_label_text, 
        font=("Arial", 12)
    )
    input_label.pack(padx=20, pady=(10, 5), anchor='w')
    
    # --- 3. 輸入框 (Entry) ---
    user_entry = tk.Entry(
        root, 
        width=60, 
        font=("Arial", 12)
    )
    user_entry.pack(padx=20, pady=(0, 20))
    
    default_value = datetime.now().strftime('%Y%m%d') # 預設顯示今日日期
    user_entry.insert(0, default_value)
    
    # --- 4. 確認按鈕 (Button) ---
    confirm_button = tk.Button(
        root,
        text="確 認 並 執 行",
        font=("Arial", 12, "bold"),
        # 點擊時，呼叫 run_full_process 函式
        command=lambda: run_full_process(root, user_entry)
    )
    confirm_button.pack(pady=10)
    
    # 啟動 Tkinter 的事件迴圈，此處會阻塞直到 root.destroy() 被呼叫
    root.mainloop()


# =================================================================
# 5. 主程式入口
# =================================================================

if __name__ == '__main__':
    WINDOW_TITLE = "股票數據分析器 - 參數輸入"
    FIXED_MESSAGE = "請輸入程式執行所需抓取資料的截止日期，或使用預設的今日日期。"
    INPUT_PROMPT = "(日期格式：YYYYMMDD，例如 20251128)"

    # 顯示 UI 視窗，並在視窗關閉後執行數據處理和結果顯示
    display_main_window_with_input(WINDOW_TITLE, FIXED_MESSAGE, INPUT_PROMPT)
    
    print("\n🎉 程式已結束運行。")