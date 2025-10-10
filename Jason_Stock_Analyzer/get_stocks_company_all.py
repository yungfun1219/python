import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import urllib3
import sys
# 確保 Pandas 處理 CSV 時，空值會被正確識別
import numpy as np 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----- 全局設定 -----
HEADERS = {
    'content-type': 'text/html; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/76.0.3809.132 Safari/537.36'
}

# 定義目標資料夾路徑
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw")
# --------------------


def list_stock(input_type):
    """爬取指定市場的股票清單，並儲存為 CSV 檔案。"""
    list_stocks = {
        'exchange': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',  # 上市
        'counter': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'  # 上櫃
    }
    
    file_path = os.path.join(OUTPUT_DIR, f'{input_type}_list.csv')
    print(f"--- 開始爬取 {input_type} 資料 ---")
    
    try:
        getdata = requests.get(list_stocks[input_type], headers=HEADERS, verify=False, timeout=10)
        getdata.encoding = 'ms950'
        soup = BeautifulSoup(getdata.text, 'html.parser')
        table = soup.find('table', class_='h4')
    except requests.exceptions.RequestException as e:
        print(f"【錯誤】網路請求失敗：{e}")
        return
    except Exception as e:
        print(f"【錯誤】處理 {input_type} 資料時發生錯誤: {e}")
        return

    if table is None:
        print(f"【注意】{input_type} 網頁找不到表格，請檢查網頁結構或 class 名稱。")
        return

    # 解析表格內容
    datalist = []
    for col in table.find_all('tr'):
        datalist.append([row.text.strip() for row in col.find_all('td')]) # 新增 .strip() 清除空白

    # 處理第一欄位合併問題 (將代號和名稱分離)
    for deal_str in datalist[1:]:
        if len(deal_str) == 7:
            parts = deal_str[0].split('\u3000', 1) 
            if len(parts) == 2:
                deal_str[0] = parts[0].strip()
                deal_str.insert(1, parts[1].strip())

    title = [
        '有價證券代號',
        '有價證券名稱',
        '國際證券辨識號碼(ISIN Code)',
        '上市日',
        '市場別',
        '產業別',
        'CFICode',
        '備註'
    ]

    df = pd.DataFrame(datalist[1:], columns=title)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f'【成功】{file_path} 已儲存。')


def combine_and_save(output_dir, file_types):
    """
    讀取多個 CSV 檔案，合併、篩選「產業別」空白的公司，並儲存為一個總檔案。
    """
    print("\n--- 開始合併檔案與數據清理 ---")
    all_data = []
    
    # 檢查並建立輸出資料夾
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"已建立資料夾: {output_dir}")
        except Exception as e:
            print(f"【錯誤】無法建立資料夾 {output_dir}: {e}")
            return
    
    for input_type in file_types:
        file_path = os.path.join(output_dir, f'{input_type}_list.csv')
        
        if not os.path.exists(file_path):
            print(f"【警告】找不到檔案：{file_path}，跳過合併。")
            continue
            
        try:
            # 讀取檔案時，將空白字串替換為 NaN，以便 Pandas 識別為缺失值
            df = pd.read_csv(file_path, na_values=['', ' '], keep_default_na=True)
            all_data.append(df)
            print(f"已讀取 {input_type}_list.csv ({len(df)} 筆資料)")
        except Exception as e:
            print(f"【錯誤】讀取 {input_type}_list.csv 時發生錯誤: {e}")

    if not all_data:
        print("沒有任何檔案可供合併。")
        return

    # 1. 合併所有 DataFrame
    combined_df = pd.concat(all_data, ignore_index=True)
    initial_rows = len(combined_df)
    
    # 2. 【核心篩選】刪除「產業別」欄位為空白（NaN 或空字串）的列
    # subset 參數指定只對 '產業別' 欄位進行空值檢查
    combined_df = combined_df.dropna(subset=['產業別'])
    
    final_rows = len(combined_df)
    
    print(f"\n數據清理結果:")
    print(f"  原始總筆數: {initial_rows}")
    print(f"  刪除無產業別筆數: {initial_rows - final_rows}")
    print(f"  最終總筆數: {final_rows}")
    
    # 3. 儲存最終檔案
    final_file_path = os.path.join(output_dir, 'stocks_all.csv')
    combined_df.to_csv(final_file_path, index=False, encoding='utf-8-sig')
    
    print("=" * 35)
    print(f"【成功】所有資料已篩選、合併並儲存至:")
    print(f"{final_file_path}")
    print("=" * 35)

    # (可選) 刪除暫存的單一市場檔案
    for input_type in file_types:
        os.remove(os.path.join(output_dir, f'{input_type}_list.csv'))
    print("已清除暫存的 exchange_list.csv 和 counter_list.csv。")


if __name__ == '__main__':
    # 1. 爬取並儲存上市/上櫃資料
    FILE_TYPES = ['exchange', 'counter']
    for stock_type in FILE_TYPES:
        list_stock(stock_type)

    # 2. 合併所有儲存的檔案並進行篩選
    combine_and_save(OUTPUT_DIR, FILE_TYPES)