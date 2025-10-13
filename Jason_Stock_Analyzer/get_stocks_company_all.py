# 【2025/10/13_上市、櫃公司股票名單】
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import urllib3
import sys
from typing import Optional
import numpy as np 
from datetime import date
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ----- 全局設定 -----
HEADERS = {
    'content-type': 'text/html; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/76.0.3809.132 Safari/537.36'
}

# 定義目標資料夾路徑
OUTPUT_log_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "logs")
OUTPUT_csv_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw")
# 定義數據清理結果的日誌檔案名稱
LOG_SUMMARY_FILENAME = 'get_company_all.log'
# --------------------

def list_stock(input_type):
    """爬取指定市場的股票清單，並儲存為 CSV 檔案。"""
    list_stocks = {
        'exchange': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',  # 上市
        'counter': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'  # 上櫃
    }
    
    file_path = os.path.join(OUTPUT_csv_DIR, f'{input_type}_list.csv')
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
            # TWSE 使用全形空格 \u3000 分隔代號與名稱
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
    讀取多個 CSV 檔案，合併、篩選「產業別」空白的公司，並儲存為一個總檔案，
    同時將清理結果寫入日誌檔案。
    
    Args:
        output_dir (str): CSV 檔案的目標儲存路徑 (datas/raw)。
        file_types (list): 爬取的市場類型清單。
    """
    print("\n--- 開始合併檔案與數據清理 ---")
    all_data = []
    
    # 檢查並建立輸出資料夾 (針對 output_dir, 也就是 datas/raw)
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
    
    # 3. 格式化數據清理結果
    summary_content = (
        f"【{date.today().strftime("%Y年%m月%d日")}_上市、櫃公司股票名單】:\n"
        f"   原始總筆數: {initial_rows}\n"
        f"   刪除無產業別筆數: {initial_rows - final_rows}\n"
        f"   最終總筆數: {final_rows}"
    )
    
    # 4. 打印清理結果到控制台
    print(f"\n{summary_content}")
    
    # 5. 寫入日誌檔案
    log_file_path = os.path.join(OUTPUT_log_DIR, LOG_SUMMARY_FILENAME)
    try:
        # 檢查並建立 logs 資料夾 (LOGS資料夾需要獨立檢查)
        if not os.path.exists(OUTPUT_log_DIR):
            os.makedirs(OUTPUT_log_DIR)
            
        with open(log_file_path, 'w', encoding='utf-8') as f:
            f.write(summary_content)
        print(f"【成功】數據清理結果已記錄至日誌檔案：{log_file_path}")
    except Exception as e:
        print(f"【錯誤】寫入日誌檔案 {LOG_SUMMARY_FILENAME} 失敗: {e}")


    # 6. 儲存最終檔案 - 修正：應使用 output_dir (即 OUTPUT_csv_DIR/datas/raw)
    final_file_path = os.path.join(output_dir, 'stocks_all.csv')
    combined_df.to_csv(final_file_path, index=False, encoding='utf-8-sig')
    
    print("=" * 35)
    print(f"【成功】所有資料已篩選、合併並儲存至:")
    print(f"{final_file_path}")
    print("=" * 35)

    # (可選) 刪除暫存的單一市場檔案
    for input_type in file_types:
        try:
            os.remove(os.path.join(output_dir, f'{input_type}_list.csv'))
        except OSError as e:
            print(f"【警告】清除暫存檔案 {input_type}_list.csv 失敗: {e}")
            continue
    print("已清除暫存的 exchange_list.csv 和 counter_list.csv。")

# --- 額外功能: 計算合併後檔案中的股票數量 (維持不變) ---
def count_stocks_in_csv(file_path: str) -> Optional[int]:
    """
    讀取指定的 CSV 檔案，並計算其中的資料列數（即股票數量）。
    Args:
        file_path: 欲讀取的 CSV 檔案的完整路徑 (str)。
    Returns:
        成功時回傳股票數量 (int)；若檔案不存在或讀取失敗則回傳 None。
    """
    
    # 1. 檢查檔案是否存在
    if not os.path.exists(file_path):
        print(f"錯誤：檔案不存在於 {file_path}")
        return None

    # 2. 嘗試讀取 CSV 檔案
    # 設定編碼優先順序：'utf-8' -> 'big5'
    encodings_to_try = ['utf-8', 'big5']
    df = None
    
    for encoding in encodings_to_try:
        try:
            # header=0 告訴 Pandas 檔案的第一行是欄位標題
            df = pd.read_csv(file_path, header=0, encoding=encoding)
            # 如果成功讀取，則跳出迴圈
            # print(f"成功使用 {encoding} 編碼讀取檔案。")
            break
        except Exception:
            # 忽略當前的編碼錯誤，嘗試下一個編碼
            continue
    
    # 3. 檢查是否成功讀取
    if df is None:
        print(f"錯誤：嘗試了 {', '.join(encodings_to_try)} 編碼，但讀取檔案失敗。")
        return None
    
    # 4. 計算股票數量並回傳
    # df.shape[0] 即為資料列數 (行數)
    stock_count = df.shape[0]
    return stock_count


if __name__ == '__main__':
    # 1. 爬取並儲存上市/上櫃資料
    FILE_TYPES = ['exchange', 'counter']
    for stock_type in FILE_TYPES:
        list_stock(stock_type)

    # 2. 合併所有儲存的檔案並進行篩選與日誌記錄
    combine_and_save(OUTPUT_csv_DIR, FILE_TYPES)
