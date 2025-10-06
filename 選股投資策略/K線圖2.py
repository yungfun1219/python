import pandas as pd
import mplfinance as mpf
import os
import sys
import matplotlib.pyplot as plt # 必須使用 Matplotlib 的 show()
import matplotlib.font_manager as fm
from datetime import datetime, timedelta
import dateutil.relativedelta

# ==========================================================
# 參數設定
# ==========================================================
FILE_PATH = r'D:\Python_repo\python\選股投資策略\K-1101.csv'
DATE_COL = '日期' 
# ----------------------------------------------------------

# --- 【修正中文顯示的核心程式碼】 ---
font_path = 'C:/Windows/Fonts/msjh.ttc'  # 微軟正黑體路徑 (Windows)

try:
    if os.path.exists(font_path):
        # 設置中文支持字型和解決負號亂碼問題
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        print("【字型設定】成功載入中文字型：Microsoft JhengHei。")
    else:
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
        plt.rcParams['axes.unicode_minus'] = False
        print("【字型警告】未找到 'Microsoft JhengHei'，嘗試使用系統備用字型。")

except Exception as e:
    print(f"【字型錯誤】設定字型失敗: {e}")
# ------------------------------------


def load_and_prepare_data(file_path, date_col):
    """載入股票數據，清洗欄位，並轉換為 mplfinance 所需的格式 (日期為索引)。"""
    print(f"\n--- 載入檔案：{os.path.basename(file_path)} ---")
    
    if not os.path.exists(file_path):
        print(f"【錯誤】找不到檔案路徑：{file_path}")
        return None
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        df.rename(columns={
            date_col: 'Date', 
            '開盤價': 'Open', 
            '最高價': 'High', 
            '最低價': 'Low', 
            '收盤價': 'Close',
            '成交股數': 'Volume'
        }, inplace=True)
        
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce') 
        
        df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
        
        if len(df) == 0:
            print("【警告】清洗後數據為空，無法繪圖。")
            return None
            
        return df

    except Exception as e:
        print(f"處理數據時發生未知錯誤：{e}")
        return None


def plot_candlestick_chart(df, title, months):
    """
    使用 mplfinance 繪製指定期間的 K 線圖，並直接在視窗中顯示。
    
    Args:
        df (pd.DataFrame): 已經過處理的股票 OHLCV 資料。
        title (str): 圖表標題。
        months (int): 繪圖期間的月數 (例如 2 或 6)。
    """
    
    # 找出 N 個月前的日期
    end_date = df.index.max()
    start_date = end_date - dateutil.relativedelta.relativedelta(months=months)
    
    # 篩選出指定期間的資料
    plot_df = df.loc[start_date:end_date]

    if len(plot_df) == 0:
        print(f"【警告】期間 ({months} 個月) 內無資料，無法繪製 {title}。")
        return
        
    print(f"\n--- 開始繪製 {title} ({len(plot_df)} 筆資料) ---")
        
    # 定義顏色和樣式
    mc = mpf.make_marketcolors(up='r', down='g', inherit=True)
    s = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
    
    # 繪製 K 線圖
    # 注意：移除了 savefig 參數
    mpf.plot(
        plot_df, 
        type='candle',             
        style=s,                   
        title=title,          
        ylabel='股價 (Price)',
        mav=(5, 20),               
        volume=True,             
        figratio=(15, 8),
        # savefig=filename <--- 移除此行
    )
    
    # 關鍵：顯示圖形視窗
    plt.show() 
    
    print(f"【成功】{title} 圖表已顯示。")


# --- 執行主要流程 ---
if __name__ == '__main__':
    data_df = load_and_prepare_data(FILE_PATH, DATE_COL)
    
    if data_df is not None:
        
        # 1. 繪製近 2 個月的 K 線圖
        plot_candlestick_chart(
            data_df, 
            title='台泥 (1101) 近 2 個月 K 線圖',
            months=2
            # 移除了 filename 參數
        )
        
        # 2. 繪製近 6 個月的 K 線圖
        plot_candlestick_chart(
            data_df, 
            title='台泥 (1101) 近 6 個月 K 線圖',
            months=6
            # 移除了 filename 參數
        )