import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# ----------------- 數據載入、清理與篩選 -----------------
file_name = r"D:\GitHub-Repo\python\選股投資策略\stock_data\raw\indi_stocks\1101_台泥_stock.csv"
df = pd.read_csv(file_name)

# 1. 處理日期欄位，並設為索引
df['日期'] = pd.to_datetime(df['日期'])
df.set_index('日期', inplace=True)

# 2. 清理「收盤價」欄位：轉換為數值型態
try:
    # 嘗試將收盤價轉換為數值
    df['收盤價'] = pd.to_numeric(df['收盤價'], errors='coerce')
except Exception:
    # 如果轉換失敗，假設是字串帶有逗號，則進行清理
    df['收盤價'] = df['收盤價'].astype(str).str.replace(',', '', regex=False)
    df['收盤價'] = pd.to_numeric(df['收盤價'], errors='coerce')

# 刪除收盤價為 NaN 的行
df.dropna(subset=['收盤價'], inplace=True)

# 3. ⭐️ 篩選近一年數據 ⭐️
# 設定結束日期為數據中的最新日期 (確保不過濾掉任何現有數據)
end_date = df.index.max()
# 計算開始日期：從最新日期往前推一年
start_date = end_date - timedelta(days=365) 

# 篩選 DataFrame
df_filtered = df[(df.index >= start_date) & (df.index <= end_date)].copy()


# ----------------- MACD 指標計算 -----------------
# 使用 pandas_ta 函式庫計算 MACD
# MACD 的計算需要足夠的歷史數據，因此我們對原始 df 進行計算，再取近一年結果
# 這麼做能確保 MACD 在起始點的計算是準確的 (EMA 需要較長的暖機期)
df.ta.macd(close='收盤價', append=True)

# 僅取出近一年的 MACD 結果
macd_df = df.loc[df_filtered.index, [
    'MACD_12_26_9',    # MACD 線
    'MACDh_12_26_9',   # 柱狀圖 (Histogram)
    'MACDs_12_26_9'    # 信號線 (Signal Line)
]].rename(columns={
    'MACD_12_26_9': 'MACD Line',
    'MACDh_12_26_9': 'MACD Histogram',
    'MACDs_12_26_9': 'Signal Line'
})

# 移除計算 MACD 初期不足夠數據的 NaN 行
macd_df.dropna(inplace=True)


# ----------------- 圖表繪製 -----------------
# 設定圖表大小
plt.figure(figsize=(15, 8))
plt.style.use('fivethirtyeight') 

# 繪製 MACD 線 和 信號線
plt.plot(macd_df.index, macd_df['MACD Line'], label='MACD Line', color='blue', linewidth=1.5)
plt.plot(macd_df.index, macd_df['Signal Line'], label='Signal Line', color='orange', linewidth=1.5)

# 繪製 MACD 柱狀圖 (Histogram)
histogram_colors = ['red' if h >= 0 else 'green' for h in macd_df['MACD Histogram']]
plt.bar(macd_df.index, macd_df['MACD Histogram'], label='Histogram', color=histogram_colors, alpha=0.6)

# 繪製零軸線 (Zero Line)
plt.axhline(0, color='grey', linestyle='--', linewidth=1)

# 設定圖表標題與標籤
plt.title(f'1101 台泥 近一年 ({start_date.strftime("%Y/%m/%d")} ~ {end_date.strftime("%Y/%m/%d")}) MACD 指標圖', fontsize=16)
plt.xlabel('日期')
plt.ylabel('MACD 值')
plt.legend(loc='best') 
plt.grid(True) 

# 格式化 X 軸日期顯示
plt.gca().xaxis.set_major_locator(plt.MaxNLocator(10)) # 只顯示 10 個刻度
plt.xticks(rotation=45) 

# 顯示圖表
plt.show()