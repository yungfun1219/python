import pandas as pd
import mplfinance as mpf
from datetime import timedelta

# ----------------- 數據載入、清理與篩選 -----------------
# 注意：為了能在通用環境中執行，我將檔案路徑改回檔案名稱。
file_name = r"D:\GitHub-Repo\python\選股投資策略\stock_data\raw\indi_stocks\1101_台泥_stock.csv"
df = pd.read_csv(file_name)

# 1. 處理日期欄位，並設為索引
df['日期'] = pd.to_datetime(df['日期'])
df.set_index('日期', inplace=True)

# 2. 清理價格和成交股數欄位：轉換為數值型態
# K 線圖需要 OHLCV (開盤價、最高價、最低價、收盤價、成交股數)
cols_to_process = {
    '開盤價': 'Open',
    '最高價': 'High',
    '最低價': 'Low',
    '收盤價': 'Close',
    '成交股數': 'Volume' # ⭐️ 關鍵修正：加入成交股數
}

for col in cols_to_process.keys():
    # 統一處理所有數值欄位（包含成交股數），清理逗號並轉為數值
    try:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    except Exception:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# 刪除任何 OHLCV 價格有 NaN 值的行
df.dropna(subset=cols_to_process.keys(), inplace=True)

# 3. 篩選近一年數據
end_date = df.index.max()
start_date = end_date - timedelta(days=365) 

# 篩選 DataFrame (使用 loc 確保保留原始日期索引)
df_filtered = df.loc[start_date:end_date].copy()

# 4. 重新命名欄位以符合 mplfinance 的標準
# ⭐️ 關鍵修正：使用包含 '成交股數' 及其對應 'Volume' 的字典進行重新命名
df_ohlc = df_filtered.rename(columns=cols_to_process)


# ----------------- K 線圖繪製 -----------------

# 設定圖表標題
chart_title = f'1101 台泥 近一年 K 線圖 ({start_date.strftime("%Y/%m/%d")} ~ {end_date.strftime("%Y/%m/%d")})'

# 繪製 K 線圖
mpf.plot(
    df_ohlc, 
    type='candle',             # K線圖類型
    style='yahoo',             # 圖表風格 
    title=chart_title,         # 圖表標題
    ylabel='價格 (新台幣)',    # Y軸標籤
    volume=True,               # 顯示成交量 (現在 DataFrame 已經有正確的 'Volume' 欄位)
    figratio=(15, 8),          # 圖表比例 (寬, 高)
    mav=(5, 20, 60),           # 添加三條移動平均線 (5日, 20日, 60日)
    show_nontrading=False,     # 不顯示非交易日
    savefig='1101_TCC_Candlestick_1Year.png' # 儲存圖片以在通用環境中顯示
)