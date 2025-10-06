import pandas as pd
import requests
import os
import time
from datetime import date

# ----------------- 設定與常數 -----------------

# 儲存資料的目的地資料夾路徑
# 請注意：此路徑是 Windows 格式，請確保 D 槽存在且路徑正確
SAVE_PATH = r'D:\Python_repo\python\選股投資策略\stock_data\raw\price_to_earnings' 

# 爬蟲時的延遲時間 (秒)，避免過快請求，建議至少 3 秒
SLEEP_TIME = 3.5

# ----------------- 資料爬取函式 (沿用上次的程式碼) -----------------

def get_yield_data(date_str):
    """
    從臺灣證券交易所 (TWSE) 取得指定日期的本益比與殖利率資料。
    
    Args:
        date_str (str): 欲查詢的日期，格式為 YYYYMMDD (例如: '20231005')
        
    Returns:
        pandas.DataFrame or None: 包含資料的 DataFrame，如果無資料則回傳 None。
    """
    
    html_url = f'https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date={date_str}&selectType=ALL'
    
    try:
        # 使用 pandas 的 read_html 函式直接讀取網頁中的表格
        tables = pd.read_html(html_url)
        
        # 經驗上，TWSE 此頁面需要的資料會是列表中的第一個元素 (索引 0)
        if tables:
            df = tables[0]
            print(f"✅ {date_str} 資料取得成功！")
            return df
        else:
            print(f"⚠️ {date_str} 的網頁中沒有找到表格，可能當日無資料。")
            return None
            
    except Exception as e:
        # 發生錯誤時（例如連線失敗、找不到網頁等），視為無資料
        print(f"❌ 爬取 {date_str} 資料時發生錯誤: {e}")
        return None

# ----------------- 主程式：自動化抓取與儲存 -----------------

def main():
    # 1. 建立日期範圍
    START_DATE = '2010-01-01'
    END_DATE = date.today().strftime('%Y-%m-%d')
    
    # 生成從開始日期到今天的日期序列，並轉換為 'YYYYMMDD' 格式的字串列表
    date_range = [
        d.strftime('%Y%m%d') 
        for d in pd.date_range(START_DATE, END_DATE, freq='D')
    ]
    
    print(f"--- 準備從 {START_DATE} 到 {END_DATE} 抓取共 {len(date_range)} 天的資料 ---")
    
    # 2. 檢查並建立儲存資料夾
    # exist_ok=True 表示如果資料夾已經存在，則不會拋出錯誤
    os.makedirs(SAVE_PATH, exist_ok=True)
    print(f"資料將儲存至: {SAVE_PATH}")

    # 3. 迴圈處理每一天的資料
    for i, current_date in enumerate(date_range):
        print(f"\n--- 正在處理第 {i+1}/{len(date_range)} 天: {current_date} ---")
        
        # 爬取資料
        df = get_yield_data(current_date)
        
        if df is not None:
            # 建立檔案名稱：日期__p_to_e.csv
            filename = f"{current_date}__p_to_e.csv"
            file_path = os.path.join(SAVE_PATH, filename)
            
            # 儲存為 CSV 檔案
            # index=False: 不寫入 DataFrame 的索引
            # encoding='utf-8-sig': 確保中文和 Excel 都能正確顯示
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"🎉 資料成功儲存為 {filename}")
        
        # 每次爬取後暫停，這是爬蟲的重要禮儀
        time.sleep(SLEEP_TIME)
        
    print("\n--- 所有日期資料處理完成 ---")

if __name__ == '__main__':
    main()