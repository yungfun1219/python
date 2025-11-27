import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import re
import mplfinance as mpf 

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
STOCK_CODE = "2330"
STOCK_NAME = "台積電" # 確保這裡使用中文繁體字
input_dir = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\1_STOCK_DAY\\" + STOCK_CODE
output_file = f"{STOCK_CODE}_stocks_data.csv"
output_path = os.path.join(os.path.dirname(input_dir), output_file) 

ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950'] 
PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
ALL_REQUIRED_COLS = ['日期'] + PRICE_COLS


# --- 資料處理部分 (略) ---
all_data = []
print(f"--- 🚀 開始處理資料夾：{input_dir} ---")

for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
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
            
            # --- 步驟 2.2: 清理所有價格欄位 ---
            for col in PRICE_COLS:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = df[col].replace(['-', 'nan', 'NaN', ''], pd.NA) 
            
            df.dropna(subset=ALL_REQUIRED_COLS, inplace=True)
            
            for col in PRICE_COLS:
                 df[col] = df[col].astype(float)
            
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

print("\n--- 📊 資料合併與排序完成 ---")

# 4. 計算移動平均線 (MA)
combined_df['MA5'] = combined_df['收盤價'].rolling(window=5, min_periods=1).mean().round(2)
combined_df['MA5'] = combined_df['MA5'].fillna(0)
combined_df['MA10'] = combined_df['收盤價'].rolling(window=10, min_periods=1).mean().round(2)
combined_df['MA10'] = combined_df['MA10'].fillna(0)
combined_df['MA20'] = combined_df['收盤價'].rolling(window=20, min_periods=1).mean().round(2)
combined_df['MA20'] = combined_df['MA20'].fillna(0)

print("--- ✅ MA5, MA10, MA20 計算完成 ---")


# 5. 另存檔案為 2330_stocks_data.csv 
combined_df['日期_str'] = combined_df['日期'].dt.strftime('%Y/%m/%d') 
output_cols = ['日期_str'] + PRICE_COLS + ['MA5', 'MA10', 'MA20']
combined_df.to_csv(output_path, index=False, encoding='utf-8-sig', columns=output_cols)
combined_df.drop(columns=['日期_str'], inplace=True) 

print(f"\n--- 🎉 資料已處理並儲存至：{output_path} ---")


# --- 6. 繪圖部分 (單張合併圖表) ---

# 計算近 90 天 (3 個月) 的起始日期
latest_date = combined_df['日期'].max()
start_date_three_months_ago = latest_date - timedelta(days=90) 

# 篩選近 90 天的資料
df_plot = combined_df[combined_df['日期'] >= start_date_three_months_ago].copy()

if df_plot.empty:
    print(f"\n⚠️ 警告：近 90 天 ({start_date_three_months_ago.strftime('%Y/%m/%d')} ~ {latest_date.strftime('%Y/%m/%d')}) 沒有足夠的資料可以繪圖。")
else:
    # --- K 線圖與 MA 合併 ---
    df_ohlc_ma = df_plot.rename(columns={
        '開盤價': 'Open',
        '最高價': 'High',
        '最低價': 'Low',
        '收盤價': 'Close'
    }).set_index('日期')

    # 設定 K 線顏色樣式 (紅漲綠跌)
    mc = mpf.make_marketcolors(
        up='r',   
        down='g', 
        edge='inherit',
        wick='inherit',
        inherit=True
    )
    s = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)

    # 設定要繪製的移動平均線
    mav = [5, 10, 20]
    
    # 標題包含中文繁體字的證券名稱 (台積電)
    chart_title = f'{STOCK_CODE} ({STOCK_NAME}) 近 90 天 K 線圖與移動平均線 (紅漲綠跌) ({df_plot["日期"].min().strftime("%Y/%m/%d")} ~ {latest_date.strftime("%Y/%m/%d")})'
    
    # 使用 mplfinance 繪製 K 線圖
    fig, axes = mpf.plot(
        df_ohlc_ma, 
        type='candle',
        mav=mav,              # 繪製 MA 線，自動生成圖例
        volume=False,
        style=s, 
        title=chart_title,    
        ylabel='價格 (TWD)',   
        figscale=1.5,
        returnfig=True,
        # 【修正點】: 移除 show_titles=True 參數
        
    )

    # 處理中文亂碼問題，確保圖表標題、Y 軸、圖例等中文元素正確顯示
    if fig and axes:
        font_name = 'Microsoft YaHei' 

        # 獲取主圖的 Axes (通常是第一個)
        main_ax = axes[0] if isinstance(axes, list) else axes
        
        # 處理標題 (包含中文繁體字 "台積電")
        if main_ax.get_title():
            # 傳遞原始標題，並指定字體
            main_ax.set_title(chart_title, fontproperties=font_name, fontsize=16) 
        
        # 處理 Y 軸標籤
        if main_ax.get_ylabel():
            main_ax.set_ylabel(main_ax.get_ylabel(), fontproperties=font_name, fontsize=12)
        
        # 處理 圖例 (Legend)
        legend = main_ax.get_legend()
        if legend:
            legend.set_visible(True)
            for text in legend.get_texts():
                text.set_fontproperties(font_name)
        
        # 確保軸刻度標籤的字體也支持中文/英文
        for tick in main_ax.get_yticklabels():
            tick.set_fontproperties(font_name)
        for tick in main_ax.get_xticklabels():
             tick.set_fontproperties(font_name)

    
    combined_chart_filename = f"{STOCK_CODE}_KLine_MA_Combined_RedUpGreenDown_90Days.png"
    fig.savefig(os.path.join(os.path.dirname(output_path), combined_chart_filename))
    print(f"📈 合併圖表 (K線+MA, 紅漲綠跌，近 90 天) 已儲存至：{os.path.dirname(output_path)}/{combined_chart_filename}")
    plt.show() 

print(f"\n🎉 總體任務完成！")