import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import urllib3
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import sys
import pathlib
from dotenv import load_dotenv # ➊ 匯入Line機器人函式庫

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================================
# 參數設定
# ==========================================================
# 股票清單 CSV 檔案路徑 (用於查詢上市日，通常是 stocks_all.csv)
BASE_DIR = pathlib.Path(__file__).resolve().parent 
LIST_FILE_PATH = BASE_DIR / "datas" / "raw" / "stocks_all.csv"
RAW_DATA_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"

# 批量處理時，此變數將被清單中的代號取代
TARGET_CODE = '1101' 
MIN_START_DATE_STR = '2025/10/01'

# 欄位名稱設定
CODE_COL = '有價證券代號'
LIST_DATE_COL = '上市日'
# ==========================================================


#從指定的 CSV 檔案中讀取並回傳所有證券代號的列表。
def get_all_stock_codes(file_path, code_col_name):
    """
    從指定的 CSV 檔案中讀取並回傳所有證券代號的列表。
    """
    print(f"--- 開始讀取所有證券代號清單：{os.path.basename(file_path)} ---")
    
    if not os.path.exists(file_path):
        print(f"【錯誤】找不到檔案路徑：{file_path}")
        return []

    try:
        df = pd.read_csv(file_path, dtype={code_col_name: str})
        
        if code_col_name not in df.columns:
            raise KeyError(f"指定的欄位名稱 '{code_col_name}' 不存在於檔案中。")
            
        # 提取欄位並轉為列表，並去除空白和空值
        stock_codes_list = df[code_col_name].str.strip().dropna().tolist()
        
        print(f"【成功】總共取得 {len(stock_codes_list)} 個證券代號準備處理。")
        return stock_codes_list

    except Exception as e:
        print(f"【錯誤】讀取或處理股票清單時發生錯誤：{e}")
        return []

# 刪除指定 CSV 檔案中屬於當前月份的所有資料。
def delete_current_month_data(file_path: str, date_column_name: str = '日期') -> bool:
    """
    刪除指定 CSV 檔案中屬於當前月份的所有資料。

    Args:
        file_path (str): 股票數據 CSV 檔案的完整路徑。
        date_column_name (str): 檔案中日期欄位的名稱 (預設為 '日期')。

    Returns:
        bool: 刪除並儲存成功返回 True，否則返回 False。
    """
    print(f"--- 開始處理檔案: {os.path.basename(file_path)} ---")

    if not os.path.exists(file_path):
        print(f"【錯誤】檔案未找到: {file_path}")
        return False

    # 1. 確定當前月份 (以當前時間為準)
    current_date = date.today()
    current_year_month = current_date.strftime('%Y%m')

    print(f"當前月份識別碼: {current_year_month}。將刪除此月份的所有資料。")
    
    try:
        # 2. 讀取 CSV 檔案
        # 假設您的 CSV 使用 utf-8-sig 編碼儲存
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if df.empty:
            print("【警告】檔案內容為空，無需操作。")
            return True

        if date_column_name not in df.columns:
            print(f"【錯誤】找不到日期欄位: '{date_column_name}'。請檢查欄位名稱。")
            return False

        # 3. 轉換日期並建立月份識別欄位
        # errors='coerce' 可將轉換失敗的值設為 NaT
        df['dt_date'] = pd.to_datetime(df[date_column_name], errors='coerce')
        
        # 建立一個 YYYYMM 格式的欄位用於篩選
        df['YYYYMM'] = df['dt_date'].dt.strftime('%Y%m')

        # 4. 篩選出【非當前月份】的資料
        # 邏輯：保留所有 YYYYMM 不等於 current_year_month 的行
        df_filtered = df[df['YYYYMM'] != current_year_month]
        
        # 5. 清理輔助欄位
        df_filtered = df_filtered.drop(columns=['dt_date', 'YYYYMM'])

        rows_deleted = len(df) - len(df_filtered)

        if rows_deleted > 0:
            # 6. 回存數據到原檔案
            df_filtered.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"✅ 成功刪除 {rows_deleted} 筆屬於 {current_year_month} 月份的資料。")
            print(f"   檔案已更新並回存: {file_path}")
            return True
        else:
            print("【訊息】檔案中沒有找到當前月份的資料，無需更新。")
            return True

    except Exception as e:
        print(f"【錯誤】在刪除數據時發生例外: {e}")
        return False

# Line機器人
def send_stock_notification(user_id, message_text):
        try:
            push_message_request = PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=message_text)]
            )
            # 注意：這裡使用全域變數 messaging_api，如果初始化失敗，這裡會報錯
            messaging_api.push_message(push_message_request) 
            print(f"訊息已成功發送給 {user_id}")
        except Exception as e:
            print(f"其他錯誤: {e}")

# 這個類別封裝了處理單一股票所需的所有邏輯和狀態
class StockDataProcessor:
    """
    處理單一股票上市日查詢、日期序列生成、歷史股價爬取與資料清洗的類別。
    """
    def __init__(self, target_code, list_file_path):
        self.target_code = target_code
        self.list_file_path = list_file_path
        self.stock_name = ''
        self.final_start_date = None
        
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        
        # 確保儲存路徑存在
        os.makedirs(RAW_DATA_DIR, exist_ok=True)


    def _get_listing_info(self):
        """讀取 CSV 找出指定證券代號的上市日期和名稱。"""
        try:
            df = pd.read_csv(self.list_file_path, dtype={CODE_COL: str})
            df[CODE_COL] = df[CODE_COL].astype(str).str.strip() 
            target_data = df[df[CODE_COL] == self.target_code]
            
            if target_data.empty:
                # 批次處理時，單一錯誤不終止程式，僅返回 None
                print(f"【跳過】在清單中找不到代號 '{self.target_code}' 的資料。")
                return None, None 
            
            found_date_str = target_data[LIST_DATE_COL].iloc[0]
            
            try:
                stock_name = target_data['有價證券名稱'].iloc[0]
            except KeyError:
                stock_name = 'Unknown'
                
            self.stock_name = stock_name
            return found_date_str
        except Exception as e:
            print(f"【錯誤】查詢代號 {self.target_code} 上市資訊時發生錯誤：{e}")
            return None


    def _generate_date_sequence(self, found_date_str):
        """判斷起始日並生成月序列 (YYYYMM 格式)。"""
        try:
            actual_listing_date = datetime.strptime(found_date_str, '%Y/%m/%d').date()
            min_start_date = datetime.strptime(MIN_START_DATE_STR, '%Y/%m/%d').date()
            
            self.final_start_date = max(actual_listing_date, min_start_date)
            
            if self.final_start_date == min_start_date:
                print(f"【注意】上市日 {found_date_str} 早於 {MIN_START_DATE_STR}，起始日設為 {MIN_START_DATE_STR}。")
            
            current_date = self.final_start_date
            today = date.today()
            date_list_yyyymm = []

            while current_date <= today:
                date_list_yyyymm.append(current_date.strftime('%Y%m'))
                current_date += relativedelta(months=1)
            
            return date_list_yyyymm

        except ValueError:
            print(f"【錯誤】代號 {self.target_code} 日期格式不正確: '{found_date_str}'。跳過此代號。")
            return None


    # (其他輔助方法 _transform_roc_date, _data_to_csv, _fetch_stock_data, _process_and_sort_data 保持不變)

    # 由於篇幅限制，以下輔助方法保持與您提供的一致
    def _transform_roc_date(self, roc_date_str):
        try:
            separator = '/' if '/' in roc_date_str else '-'
            Y, m, d = roc_date_str.split(separator)
            return f"{int(Y) + 1911}/{m.zfill(2)}/{d.zfill(2)}"
        except Exception:
            return roc_date_str

    def _data_to_csv(self, input_df):
        file_name = f'{self.target_code}_{self.stock_name}_stock.csv'
        file_path = os.path.join(RAW_DATA_DIR, file_name)
        
        if not os.path.exists(file_path):
            input_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f'-> 建立新資料並寫入完成！')
            return

        try:
            current_data = pd.read_csv(file_path, encoding='utf-8-sig')
            new_latest_date = input_df['日期'].iloc[0]
            
            if new_latest_date in current_data['日期'].tolist():
                print('-> 資料檢查結果：有重複資料，不重複寫入。')
            else:
                input_df.to_csv(file_path, mode='a', index=False, header=False, encoding='utf-8-sig')
                print('-> 資料檢查結果：無重複資料，寫入完成！')
        except Exception as e:
            print(f'【錯誤】代號 {self.target_code} 寫入時發生錯誤: {e}')
        
        time.sleep(1)

    def _fetch_stock_data(self, search_year_month):
        print(f"\n---> 爬取月份: {search_year_month}")
        baseurl = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={search_year_month}01&stockNo={self.target_code}"
        
        try:
            data = requests.get(url=baseurl, headers=self.headers, verify=False, timeout=15).content
            soup = BeautifulSoup(data, 'html.parser')
            
            if '很抱歉，沒有符合條件的資料!' in soup.text:
                print('⚠ 網頁無資料，略過此月份。')
                return None
            
            table = soup.find('table')
            if table is None:
                print('⚠ 找不到表格，網頁結構可能已變更。')
                return None
                
            thead = table.find('thead')
            tbody = table.find('tbody')
            columns = [th.text.strip() for th in thead.find_all('th')][1:]
            
            datalist = []
            for tr in tbody.find_all('tr'):
                tds = [td.text.strip() for td in tr.find_all('td')]
                if tds:
                    tds[0] = self._transform_roc_date(tds[0])
                    datalist.append(tds)

            df = pd.DataFrame(datalist, columns=columns)
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"【錯誤】網路請求失敗: {e}")
            return None
        except Exception as e:
            print(f"【錯誤】爬取 {search_year_month} 資料時發生錯誤: {e}")
            return None

    def _process_and_sort_data(self):
        file_name = f'{self.target_code}_{self.stock_name}_stock.csv'
        file_path = os.path.join(RAW_DATA_DIR, file_name)
        
        try:
            df = pd.read_csv(file_path)
            date_column = '日期' 
            df[date_column] = pd.to_datetime(df[date_column])
            df_sorted = df.sort_values(by=date_column, ascending=True).drop_duplicates()
            df_sorted[date_column] = df_sorted[date_column].dt.strftime('%Y/%m/%d')
            df_sorted.reset_index(drop=True, inplace=True)
            df_sorted.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"【成功】資料已清理、排序並回存至檔案：{file_name}")
        except Exception as e:
            print(f"【錯誤】清理或儲存檔案時發生錯誤：{e}")


    def run(self):
        """執行單一股票的資料獲取和處理流程。"""
        
        # 1. 查詢上市日並生成月份序列
        found_date_str = self._get_listing_info()
        
        # 如果無法取得上市資訊，則終止此股票的處理
        if found_date_str is None:
            return 
            
        print(f"\n--- 開始處理股票: {self.target_code} ({self.stock_name}) ---")
        
        month_list = self._generate_date_sequence(found_date_str)
        
        # 如果日期序列生成失敗，則終止此股票的處理
        if month_list is None:
            return
            
        print(f"總共需爬取 {len(month_list)} 個月份的資料，從 {month_list[0]} 到 {month_list[-1]}。")
        print("=" * 50)
        
        # 2. 依月份序列爬取資料
        for search_month in month_list:
            df_month = self._fetch_stock_data(search_month)
            if df_month is not None:
                self._data_to_csv(df_month)
            time.sleep(5) # 設置合理的延遲

        print("=" * 50)
        # 3. 最終清理與排序
        self._process_and_sort_data()
        
        print(f"【程序完成】股票 {self.target_code} ({self.stock_name}) 的歷史資料已更新。\n")

# ----------------------------------------------------------------------
# 批次執行主程序
# ----------------------------------------------------------------------
if __name__ == '__main__':
    
    # --------  Line機器人訊息 -------
    # ➋ 載入 line_API.env 檔案中的變數
    # 注意：如果您使用 .env 以外的檔名 (如 line_token.env)，需要指定檔名
    load_dotenv(r"D:\Python_repo\python\Jason_Stock_Analyzer\line_API.env") 

    # ➌ 從環境變數中讀取 Token 和 User ID
    LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    LINE_USER_ID = os.getenv("LINE_USER_ID")


    # 修正 LineBotApiError 的匯入路徑（根據您上一個問題的解答）
    from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
    from linebot.v3.messaging import TextMessage, PushMessageRequest


    # ----------------- 檢查 Token 是否存在 -----------------
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
        exit()

    try:
        # 初始化 Configuration 和 MessagingApi
        configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
        api_client = ApiClient(configuration)
        messaging_api = MessagingApi(api_client)
        print("Line Bot API 初始化成功。")
    except Exception as e:
        print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")

#--------------------------------

    # 1. 取得所有股票代號清單
    all_codes = get_all_stock_codes(LIST_FILE_PATH, CODE_COL)
    
    print(all_codes)
    if not all_codes:
        print("沒有有效的股票代號清單，程式終止。")
        sys.exit(1)
        
    total_stocks = len(all_codes)
    processed_count = 0

    print(f"\n--- 準備開始批量處理 {total_stocks} 檔股票 ---")

    # 2. 遍歷清單，逐一處理每檔股票
    for code in all_codes:
        processed_count += 1
        print(f"\n***** 處理進度: {processed_count}/{total_stocks} (代號: {code}) *****")
        
        # 實例化 StockDataProcessor 類別，TARGET_CODE 變數被當前迴圈的 code 取代
        processor = StockDataProcessor(
            target_code=code,
            list_file_path=LIST_FILE_PATH
        )
        #FILE_TO_CLEAN = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\1_STOCK_DAY\1101_台泥_stock.csv"
        #file_name = f"{code}_{CODE_COL}_stock.csv"
        #FILE_TO_CLEAN = RAW_DATA_DIR / file_name

        # 執行清除操作
        success = delete_current_month_data(LIST_FILE_PATH)

        # 判斷執行結果
        if success:
            print("\n資料清理程序執行完成。")
        else:
            print("\n資料清理程序執行失敗。")


        # 執行單一股票的完整流程
        processor.run()
        
        # 為了避免連續請求過快，在處理完一檔股票後增加較長的延遲
        time.sleep(5) 
         
        
    print("\n======================================================")
    print("所有股票數據批量處理任務已完成！")
    print("========================================================")

     # Line機器人範例執行
    analysis_report = f"所有股票數據批量抓取任務已完成！"
    send_stock_notification(LINE_USER_ID, analysis_report)
