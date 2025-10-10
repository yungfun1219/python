import pandas as pd
import requests
import os
from datetime import datetime
import time 
import urllib3 

# --- 設定參數 ---
# 股價資料來源檔案 (請依實際路徑修改)
# 這裡假設您的股價檔案在執行目錄的上一層 'stock_data/raw/indi_stocks/' 內
# 如果您的檔案路徑是固定的，請換回絕對路徑：r'D:\Python_repo\python\選股投資策略\stock_data\raw\indi_stocks\1101_台泥_stock.csv'
# 為了通用性，這裡使用相對路徑，請確保路徑正確
SOURCE_STOCK_FILE = os.path.join(
    os.path.dirname(os.getcwd()),  # 假設上一層目錄
    'datas', 'raw', 'indi_stocks', '1101_台泥_stock.csv'
)

# 殖利率資料儲存目錄 (會建立在執行程式的當前目錄下)
TARGET_DIR = os.path.join(os.getcwd(), 'twse_yield_data')

DATE_COL = '20251009' 
SLEEP_TIME_SEC = 5 # 每次抓取間隔秒數

# 關閉因為 verify=False 而產生的 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 步驟 1: 提取並轉換日期 ---
def extract_dates_from_csv(file_path: str, date_col: str) -> list[str]:
    """
    從本地 CSV 檔案中讀取日期欄位，並轉換為 YYYYMMDD 格式列表。
    """
    if not os.path.exists(file_path):
        print(f"【錯誤】找不到股價資料來源檔案：{file_path}")
        return []

    try:
        # 使用 utf-8-sig 處理 Excel 匯出的 CSV 檔
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        if date_col not in df.columns:
            print(f"【錯誤】CSV 檔案中找不到指定的日期欄位：'{date_col}'")
            return []

        # 將日期欄位轉為 datetime 物件
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # 移除無效日期，並轉換為 TWSE 所需的 YYYYMMDD 字串格式
        dates = df[date_col].dropna().dt.strftime('%Y%m%d').unique().tolist()
        
        print(f"【成功】從股價檔案中提取到 {len(dates)} 個不重複的日期。")
        
        return dates

    except Exception as e:
        print(f"【錯誤】讀取或處理股價檔案中的日期時發生錯誤：{e}")
        return []

# --- 步驟 2: 抓取 TWSE 殖利率資料 ---
def get_twse_yield_data(target_date: str):
    """
    從 TWSE 抓取特定日期的殖利率資料。
    """
    html_url = f'https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date={target_date}&selectType=ALL'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }
    
    try:
        # 1. 確保連線並忽略 SSL 驗證
        response = requests.get(url=html_url, headers=headers, verify=False, timeout=15)
        response.raise_for_status() # 對於非 200 的狀態碼拋出異常
        response.encoding = 'utf-8' 
        response_text = response.text
        
        # 2. 檢查網頁內容是否為「查無資料」
        if "查無資料" in response_text:
            print(f"  > 日期 {target_date}: 查無資料 (可能為非交易日)。")
            return None
        
        # 3. 嘗試解析表格
        tables = pd.read_html(response_text)
        
        if not tables:
            print(f"  > 日期 {target_date}: 網頁上找不到表格。")
            return None
        
        df_yield = tables[0]
        
        # 統一欄位名稱
        df_yield.columns = [
            '證券代號', '證券名稱', '除息交易日', '發放股利年度', 
            '現金股利', '殖利率(%)', '股價淨值比', '資料日期'
        ]
        
        print(f"  > 日期 {target_date}: 成功抓取 {len(df_yield)} 筆資料。")
        return df_yield
        
    except requests.exceptions.RequestException:
        print(f"  > 日期 {target_date}: 網路請求失敗或超時。")
    except ValueError:
        print(f"  > 日期 {target_date}: 無法解析表格內容。")
    except Exception as e:
        print(f"  > 日期 {target_date}: 發生未知錯誤：{e}")
        
    return None

# --- 步驟 3: 主要執行流程 ---
def main():
    
    # 確保目標儲存資料夾存在
    os.makedirs(TARGET_DIR, exist_ok=True)
    print(f"【儲存路徑】已確保目標資料夾存在：{TARGET_DIR}")

    # 1. 獲取所有目標日期
    all_dates = extract_dates_from_csv(SOURCE_STOCK_FILE, DATE_COL)
    
    if not all_dates:
        print("【流程結束】因日期列表為空，停止執行。")
        return

    # 排序日期，從舊到新抓取
    all_dates.sort()
    
    success_count = 0
    
    print("\n--- 開始迭代抓取 TWSE 殖利率資料 ---")
    
    for date_str in all_dates:
        
        # 檢查檔案是否已存在，若存在則跳過，避免重複抓取
        target_file = os.path.join(TARGET_DIR, f'{date_str}.csv')
        if os.path.exists(target_file):
            # print(f"  > 日期 {date_str}: 檔案已存在，跳過。")
            continue

        # 抓取資料
        df_result = get_twse_yield_data(date_str)
        
        if df_result is not None and not df_result.empty:
            # 將結果儲存為 CSV
            try:
                # 使用 utf-8-sig 以便 Excel 正確開啟中文
                df_result.to_csv(target_file, index=False, encoding='utf-8-sig')
                success_count += 1
            except Exception as e:
                print(f"  > 日期 {date_str}: 儲存檔案失敗：{e}")
        
        # 依設定間隔，暫停一段時間
        time.sleep(SLEEP_TIME_SEC) 
    
    print(f"\n--- 批量抓取完成 ---")
    print(f"總共嘗試 {len(all_dates)} 個日期。")
    print(f"成功儲存 {success_count} 個新檔案至 {TARGET_DIR}")

if __name__ == '__main__':
    main()