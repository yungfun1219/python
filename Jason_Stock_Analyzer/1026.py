import pandas as pd
from dotenv import load_dotenv # ➊ 匯入Line機器人函式庫
import os
from datetime import date
from pathlib import Path
from typing import Union
import schedule
from datetime import datetime
import time
import keyboard  # 新增: 用於偵測鍵盤輸入
import requests
from io import StringIO
import urllib3
import re
from typing import Optional, Tuple, List
import utils.jason_utils as jutils
from google import genai
from google.genai.errors import APIError

# 檔案路徑

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = BASE_DIR / "datas" / "raw" / "4_MI_INDEX20"

#TARGET_DATE = date.today().strftime("%Y%m%d") 
TARGET_DATE = "20251007"  # 測試用特定日期

file_path = OUTPUT_DIR / f"{TARGET_DATE}_MI_INDEX20_Market.csv"

# --------  Line機器人訊息 -------
# ➋ 載入 line_API.env 檔案中的變數
# 注意：如果您使用 .env 以外的檔名 (如 line_token.env)，需要指定檔名
line_token_file = BASE_DIR / "line_API.env"
load_dotenv(line_token_file) 
#load_dotenv(r"D:\Python_repo\python\Jason_Stock_Analyzer\line_API.env") 


# ➌ 從環境變數中讀取 Token 和 User ID
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_USER_ID = os.getenv("LINE_USER_ID")


# 修正 LineBotApiError 的匯入路徑（根據您上一個問題的解答）
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
from linebot.v3.messaging import TextMessage, PushMessageRequest

# google-gemini AI詢問
def get_gemini_response(prompt_text):
    # 此處不再需要 os.getenv()，因為 load_dotenv() 已經將金鑰載入到系統環境變數中
    try:
        # ➌ Client() 會自動偵測並使用環境變數中的 GEMINI_API_KEY
        client = genai.Client() 
        
        # ... 後續程式碼與之前範例相同 ...
        model_name = 'gemini-2.5-flash'
        print(f"\n--- 正在使用 {model_name} 詢問 Gemini ---")
        
        response = client.models.generate_content(
            model=model_name,
            contents=prompt_text
        )
        return response.text

    except APIError as e:
        return f"Gemini API 錯誤: {e}"
    except Exception as e:
        return f"發生錯誤: {e}"

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


# 檢查是否為休市日
def check_date_in_csv(file_path: str, date_to_check: str, date_column_name: str = '日期') -> Union[bool, pd.Series]:
    """
    檢查指定日期字串是否出現在 CSV 檔案的特定欄位中。
    
    Args:
        file_path (str): holidays_all.csv 檔案的完整路徑。
        date_to_check (str): 要檢查的日期字串，例如 '2025/10/10'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
        
    Returns:
        Union[bool, pd.Series]: 如果找到，返回包含匹配行的 Series (布林值)，
                                 如果未找到或檔案不存在，返回 False。
    """
    print(f"🔍 正在檢查檔案: {os.path.basename(file_path)}")
    print(f"目標日期: {date_to_check}")

    if not os.path.exists(file_path):
        print("【錯誤】檔案路徑不存在，請確認路徑是否正確。")
        return False
        
    try:
        # 由於 holidays_all.csv 是由您程式碼最後儲存的，
        # 且您儲存時使用 encoding='utf-8-sig'，這裡也使用相同的編碼讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            print(f"檔案中的欄位有: {df.columns.tolist()}")
            return False

        # 使用向量化操作 (isin) 檢查欄位中是否包含目標日期
        # 即使欄位類型是 object (字串)，也能正確檢查
        is_present = df[date_column_name].isin([date_to_check])
        
        if is_present.any():
            # 找到匹配的行
            matched_rows = df[is_present]
            print(f"✅ 日期 '{date_to_check}' 已在檔案中找到！")
            print("--- 匹配的資料列 ---")
            print(matched_rows)
            return True
        else:
            print(f"❌ 日期 '{date_to_check}' 未在檔案的 '{date_column_name}' 欄位中找到。")
            return False

    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return False
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return False

# 抓取資料 --------------
def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """
    Args:
        response_text: HTTP 請求回傳的文字內容 (Big5 編碼)。
        header_row: CSV 檔案中資料表頭所在的行數 (0-indexed)。
    Returns:
        Optional[pd.DataFrame]: 處理後的 DataFrame。
    """
    try:
        csv_data = StringIO(response_text)
        # 嘗試讀取 CSV
        df = pd.read_csv(
            csv_data, 
            header=header_row,          # 資料表頭所在的行數
            skipinitialspace=True,      # 跳過分隔符後的空格
            on_bad_lines='skip',        # 跳過格式不正確的行
            encoding='Big5'             # 使用 Big5 編碼讀取
        )
        # TWSE 的 CSV 欄位名稱常有隱藏空格，導致 df.columns 無法正確匹配。
        if not df.empty:
            df.columns = df.columns.str.strip()
        # 移除所有欄位皆為空的行
        df = df.dropna(how='all')
        # 移除資料尾部可能出現的彙總或備註行
        if not df.empty and df.iloc[-1].astype(str).str.contains('合計|總計|備註', na=False).any():
            df = df.iloc[:-1]
        return df
    except Exception as e:
        print(f"在讀取或清理 CSV 數據時發生錯誤: {e}")

        return None

# 共用的輔助函式，用於發送 HTTP 請求並檢查狀態。
def _fetch_twse_data(url: str) -> Optional[str]:
    """
    Args:
        url: 完整的 TWSE 資料 URL。
    Returns:
        Optional[str]: 成功獲取後，以 Big5 解碼的文字內容。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Big5'
        return response.text
    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP 錯誤：{errh} (該日可能無交易資料)")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")

    return None
# 檢查檔案是否存在且確實是一個檔案 (非資料夾)
def check_folder_and_create(folder_path: str):
    """
    參數:
        file_path (str): 要檢查的檔案路徑。
    回傳:
        bool: 檔案存在時回傳 True；否則回傳 False。
    """
    OUTPUT_DIR, filename_new = jutils.get_path_to_folder_file(folder_path)
    jutils.check_and_create_folder(OUTPUT_DIR)
    jutils.check_file_exists(filename_new)
    return True

def fetch_twse_mi_index20(target_date: str) -> Optional[pd.DataFrame]:
    """
    (4/10) 抓取指定日期的 MI_INDEX20 報告 (收盤指數及成交量值資訊)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20
    
    **修正:** 表頭索引改為 2 (原為 1)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20"
    url = f"{base_url}?date={target_date}&response=csv"
        
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "4_MI_INDEX20")
    filename = OUTPUT_DIR + f"\\{target_date}_MI_INDEX20_Market.csv"
    
    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (4/10) MI_INDEX20 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None
    
    # MI_INDEX20 報表的表頭在索引 2
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '證券代號' in df.columns:
        df = df[df['證券代號'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (4/10) {filename} 儲存成功。")
        return df
    return None

def main_run():

    # 先抓取資料
    fetch_twse_mi_index20(TARGET_DATE)

    # ----------------- 檢查 Token 是否存在 -----------------

    # 您指定的檔案路徑

    FILE_PATH = BASE_DIR / "datas" / "processed" / "get_holidays" / "holidays_all.csv"
    DATE_TO_CHECK = date.today().strftime("%Y/%m/%d") 
    #DATE_TO_CHECK = '2025/10/07' 
    DATE_COLUMN = '日期' # 根據您前面的程式碼，合併後的欄位名稱應為 '日期'

    # 執行檢查
    result_found = check_date_in_csv(FILE_PATH, DATE_TO_CHECK, DATE_COLUMN)

    # if not LINE_CHANNEL_ACCESS_TOKEN:
    #     print("錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
    #     exit()

    # try:
    #     # 初始化 Configuration 和 MessagingApi
    #     configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
    #     api_client = ApiClient(configuration)
    #     messaging_api = MessagingApi(api_client)
    #     print("Line Bot API 初始化成功。")
    # except Exception as e:
    #     print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")
    #     return

    # ... 接下來的程式碼保持不變 ...
    # 這是接收訊息的用戶 ID 或群組 ID
    # LINE_USER_ID 現在已經從 .env 檔案中讀取

    # Line機器人範例執行
    #analysis_report = "台積電 (2330) 近期走勢強勁，RSI 位於 65，預期短期內仍有上漲動能。"
    #send_stock_notification(LINE_USER_ID, analysis_report)
    #--------------------------------

    try:
        # 讀取 CSV 檔案
        # 假設檔案是 Big5 編碼 (台灣常見)，若遇到編碼錯誤可嘗試 'utf-8' 或 'cp950'
        df = pd.read_csv(file_path, encoding='utf-8')

        # 確保【證券代號】是以字串形式處理，以便於長度篩選
        df['證券代號'] = df['證券代號'].astype(str).str.strip()
        
        # --- 篩選條件 ---
        
        # 1. 取得【證券代號】為4碼數字的廠商
        # 使用 .str.match() 配合正則表達式來檢查是否為四位數字
        df_filtered = df[df['證券代號'].str.match(r'^\d{4}$', na=False)]
        
        # 2. 取得【漲跌(+/-)】欄位為【+】
        # 使用 .str.strip() 來移除可能的空白字元，確保精確匹配
        df_filtered = df_filtered[df_filtered['漲跌(+/-)'].astype(str).str.strip() == '+']
        
        # 選擇需要的欄位：【證券代號】、【證券名稱】
        result = df_filtered[['證券代號', '證券名稱']]
        
        for i in result:
            print(i)

        # 顯示結果
        # if not result.empty:
        #     print("符合條件的廠商資料如下：")
        #     print("--------------------")
        #     print(result.to_string(index=False))
        #     print(f"\n總共找到 {len(result)} 筆資料。")

        #     analysis_report = f"""{DATE_TO_CHECK}交易前20家廠商資料如下：
        #         \n{result.to_string(index=False)}
        #         \n總共 {len(result)} 家。
        # """
        # else:
        #     print("未找到符合所有篩選條件的資料。")

    except FileNotFoundError:
        print(f"錯誤：找不到檔案路徑 {file_path}")
    except UnicodeDecodeError as e:
        print(f"錯誤：檔案編碼問題。請嘗試修改 read_csv 中的 encoding 參數 (例如 'utf-8' 或 'cp950')。詳細錯誤：{e}")
    except KeyError as e:
        print(f"錯誤：CSV 檔案中缺少必要的欄位。請檢查欄位名稱是否正確。詳細錯誤：{e}")
    except Exception as e:
        print(f"發生了一個未預期的錯誤：{e}")

    # AI詢問
    if not os.getenv("GEMINI_API_KEY"):
         print("錯誤：未能從 gemini_API.env 載入 GEMINI_API_KEY。請檢查檔案內容和名稱。")
         exit()
    #user_input = f"今天是{DATE_TO_CHECK}，請幫我蒐集股票力積電(6770)的相關新聞！並簡短使用300字做說明"
    user_input = f"今天是{DATE_TO_CHECK}，請幫我提供美國股市漲跌"
    # user_input = input("請輸入您的詢問文字: \n")
    if user_input.strip():
        gemini_reply = get_gemini_response(user_input)
        print("\n================== Gemini 回覆 ==================")
        print(gemini_reply)
        print("=================================================")
    else:
        print("輸入不能為空。")
    # -----------------------

    if result_found:
        analysis_report = f"今天是2025/10/26，請幫我提供美國股市漲跌"
        #send_stock_notification(LINE_USER_ID, analysis_report)
    else:
        send_stock_notification(LINE_USER_ID, analysis_report)

#---------------

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



# 先運行 schedule.clear() 將排程清除，避免習慣使用 jupyter notebook 整合開發環境的讀者，
# 有殘存的排程，造成運行結果不如預期
#schedule.clear()

# 指定每 15 秒運行一次 say_hi 函數
schedule.every(5).seconds.do(main_run)

# 每天 15:00 運行一次 get_price 函數
#schedule.every().day.at('17:00').do(main_run)

# 將 schedule.run_pending() 放在 while 無窮迴圈內
while True:
    schedule.run_pending()
    time.sleep(1) # 增加 time.sleep(1) 減少 CPU 負載

