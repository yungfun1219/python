import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import urllib3
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import sys

# 關閉 SSL 警告 (通常用於處理某些網站的憑證問題)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================================
# 參數設定
# 調整這些變數以符合您的檔案路徑和目標股票
# ==========================================================
# 股票清單 CSV 檔案路徑 (用於查詢上市日)
LIST_FILE_PATH = r'D:\Python_repo\python\選股投資策略\stock_data\raw\stocks_all.csv'
TARGET_CODE = '1101'
MIN_START_DATE_STR = '2010/01/01'

# 資料夾設定
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + os.sep
RAW_DATA_DIR = os.path.join(BASE_DIR, 'stock_data', 'raw', 'indi_stocks')

# 欄位名稱設定 (需與 LIST_FILE_PATH 中的 CSV 欄位名稱一致)
CODE_COL = '有價證券代號'
LIST_DATE_COL = '上市日'
# ----------------------------------------------------------

class StockDataProcessor:
    """
    處理股票上市日查詢、日期序列生成、歷史股價爬取與資料清洗的類別。
    """
    def __init__(self, target_code, list_file_path):
        self.target_code = target_code
        self.list_file_path = list_file_path
        self.stock_name = '' # 將在查詢上市日後嘗試獲取
        self.final_start_date = None
        
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        
        # 確保儲存路徑存在
        os.makedirs(RAW_DATA_DIR, exist_ok=True)


    def _get_listing_info(self):
        """讀取 CSV 找出指定證券代號的上市日期和名稱。"""
        try:
            df = pd.read_csv(self.list_file_path)
            df[CODE_COL] = df[CODE_COL].astype(str).str.strip() 
            target_data = df[df[CODE_COL] == self.target_code]
            
            if target_data.empty:
                print(f"【錯誤】在檔案中找不到代號 '{self.target_code}' 的資料。")
                sys.exit(1)
            
            # 取得上市日期字串 (例如 '1990/12/14')
            found_date_str = target_data[LIST_DATE_COL].iloc[0]
            
            # 嘗試取得股票名稱，若欄位不存在則設為空
            try:
                self.stock_name = target_data['有價證券名稱'].iloc[0]
            except KeyError:
                self.stock_name = 'Unknown'
                
            return found_date_str

        except Exception as e:
            print(f"【錯誤】查詢上市資訊時發生錯誤：{e}")
            sys.exit(1)


    def _generate_date_sequence(self, found_date_str):
        """判斷起始日並生成月序列 (YYYYMM 格式)。"""
        try:
            actual_listing_date = datetime.strptime(found_date_str, '%Y/%m/%d').date()
            min_start_date = datetime.strptime(MIN_START_DATE_STR, '%Y/%m/%d').date()
            
            # 設定最終起始日期
            self.final_start_date = max(actual_listing_date, min_start_date)
            
            if self.final_start_date == min_start_date:
                print(f"【注意】上市日 {found_date_str} 早於 {MIN_START_DATE_STR}，起始日設為 {MIN_START_DATE_STR}。")
            else:
                print(f"起始日設為實際上市日: {found_date_str}")
            
            current_date = self.final_start_date
            today = date.today()
            date_list_yyyymm = []

            # 迴圈進行月份遞增計算，直到今天
            while current_date <= today:
                date_list_yyyymm.append(current_date.strftime('%Y%m'))
                current_date += relativedelta(months=1)
            
            return date_list_yyyymm

        except ValueError:
            print(f"【錯誤】日期字串格式不正確: '{found_date_str}' 或 '{MIN_START_DATE_STR}'。請檢查格式。")
            sys.exit(1)


    def _transform_roc_date(self, roc_date_str):
        """將民國年日期 (XX/XX/XX) 轉換為西元年 (YYYY/MM/DD)。"""
        try:
            # 假設輸入格式是 '110/01/01' 或 '110-01-01'
            separator = '/' if '/' in roc_date_str else '-'
            Y, m, d = roc_date_str.split(separator)
            return f"{int(Y) + 1911}/{m.zfill(2)}/{d.zfill(2)}" # 補零確保格式一致
        except Exception:
            return roc_date_str # 轉換失敗時回傳原始字串

    
    def _data_to_csv(self, input_df):
        """寫入或追加資料到 CSV 檔案，並檢查重複資料。"""
        
        file_name = f'{self.target_code}_{self.stock_name}_stock.csv'
        file_path = os.path.join(RAW_DATA_DIR, file_name)
        
        if not os.path.exists(file_path):
            # 檔案不存在，建立新檔案
            print('建立新資料...')
            input_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print('寫入完成!')
            return

        # 檔案存在，檢查重複並追加
        try:
            current_data = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # 檢查最新的日期是否已存在於現有資料中
            new_latest_date = input_df['日期'].iloc[0]
            
            if new_latest_date in current_data['日期'].tolist():
                print('資料檢查結果：有重複資料...不重複寫入!')
            else:
                print('資料檢查結果：無重複資料...寫入中...')
                # 追加模式，不寫入 header
                input_df.to_csv(file_path, mode='a', index=False, header=False, encoding='utf-8-sig')
                print('寫入完成!')
        except Exception as e:
            print(f'【錯誤】資料檢查或寫入時發生錯誤: {e}')
        
        time.sleep(1) # 暫停一下避免過快

    
    def _fetch_stock_data(self, search_year_month):
        """爬取單一月份的歷史股價資料。"""
        
        print(f"\n---> 爬取月份: {search_year_month}")
        baseurl = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={search_year_month}01&stockNo={self.target_code}"
        
        try:
            data = requests.get(url=baseurl, headers=self.headers, verify=False, timeout=15).content
            soup = BeautifulSoup(data, 'html.parser')

            # 檢查網頁回應是否包含表格
            if '很抱歉，沒有符合條件的資料!' in soup.text:
                print('⚠ 網頁無資料，略過此月份。')
                return None
            
            table = soup.find('table')
            if table is None:
                print('⚠ 找不到表格，網頁結構可能已變更。')
                return None
                
            thead = table.find('thead')
            tbody = table.find('tbody')

            # 取得欄位名稱 (從第二個開始，因為第一個通常是空或不需要)
            columns = [th.text.strip() for th in thead.find_all('th')][1:]
            
            # 取得表格資料
            datalist = []
            for tr in tbody.find_all('tr'):
                tds = [td.text.strip() for td in tr.find_all('td')]
                if tds:
                    tds[0] = self._transform_roc_date(tds[0])  # 民國年轉西元年
                    datalist.append(tds)

            # 轉成 DataFrame
            df = pd.DataFrame(datalist, columns=columns)
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"【錯誤】網路請求失敗: {e}")
            return None
        except Exception as e:
            print(f"【錯誤】爬取 {search_year_month} 資料時發生錯誤: {e}")
            return None


    def _process_and_sort_data(self):
        """讀取 CSV 檔案，統一日期格式、依日期排序，並回存。"""
        
        file_name = f'{self.target_code}_{self.stock_name}_stock.csv'
        file_path = os.path.join(RAW_DATA_DIR, file_name)
        
        print(f"\n--- 開始清理與排序檔案：{file_name} ---")
        
        try:
            df = pd.read_csv(file_path)
            date_column = '日期' # 假設爬蟲回傳的欄位名稱是 '日期'

            # 1. 轉換日期為 datetime 物件
            df[date_column] = pd.to_datetime(df[date_column])
            
            # 2. 進行日期排序
            df_sorted = df.sort_values(by=date_column, ascending=True).drop_duplicates()

            # 3. 統一日期格式並重設索引
            df_sorted[date_column] = df_sorted[date_column].dt.strftime('%Y/%m/%d')
            df_sorted.reset_index(drop=True, inplace=True)
            
            # 4. 回存到原檔案
            df_sorted.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"【成功】資料已清理、排序並回存至檔案：{file_path}")
            
        except Exception as e:
            print(f"【錯誤】清理或儲存檔案時發生錯誤：{e}")
            

    def run(self):
        """執行整個資料獲取和處理流程。"""
        
        print(f"--- 啟動股票數據處理程序 (代號: {self.target_code}) ---")
        
        # 1. 查詢上市日並生成月份序列
        found_date_str = self._get_listing_info()
        month_list = self._generate_date_sequence(found_date_str)
        
        print(f"總共需爬取 {len(month_list)} 個月份的資料，從 {month_list[0]} 到 {month_list[-1]}。")
        print("=" * 50)
        
        # 2. 依月份序列爬取資料
        for search_month in month_list:
            df_month = self._fetch_stock_data(search_month)
            if df_month is not None:
                self._data_to_csv(df_month)
            time.sleep(5) # 設置合理的延遲，避免被網站封鎖
            
        print("=" * 50)
        # 3. 最終清理與排序
        self._process_and_sort_data()
        
        print(f"\n【程序完成】股票 {self.target_code} 的歷史資料已更新。")


# 執行主程序
if __name__ == '__main__':
    processor = StockDataProcessor(
        target_code=TARGET_CODE,
        list_file_path=LIST_FILE_PATH
    )
    processor.run()