import pandas as pd
from dotenv import load_dotenv 
import os
from datetime import date, datetime 
from pathlib import Path
import schedule
import time
import requests
from io import StringIO
import re
from typing import Optional, Union
# 假設您有一個名為 utils.jason_utils 的輔助模組
import utils.jason_utils as jutils 
from google import genai
from google.genai.errors import APIError

# --- LINE Bot 相關匯入 (V3 SDK) ---
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, TextMessage, 
    PushMessageRequest,
    FlexMessage 
)

# ---------------------------- 參數設置與 API 初始化 ----------------------------

# 檔案路徑設置
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = BASE_DIR / "datas" / "raw" / "4_MI_INDEX20"
HOLIDAY_FILE_PATH = BASE_DIR / "datas" / "processed" / "get_holidays" / "holidays_all.csv"
HOLIDAY_DATE_COLUMN = '日期'

# 日期設置
TARGET_DATE = date.today().strftime("%Y%m%d") 
DATE_TO_CHECK_HOLIDAY = date.today().strftime("%Y/%m/%d") 
file_path = OUTPUT_DIR / f"{TARGET_DATE}_MI_INDEX20_Market.csv"

# 載入環境變數
line_token_file = BASE_DIR / "line_API.env"
load_dotenv(line_token_file) 
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_USER_ID = os.getenv("LINE_USER_ID")

# --- LINE API 初始化 (修正 NameError: Configuration) ---
if not LINE_CHANNEL_ACCESS_TOKEN:
    print("❌ 錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
    exit()

try:
    configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
    api_client = ApiClient(configuration)
    # 設置為全域變數，供所有發送函式使用
    messaging_api = MessagingApi(api_client) 
    print("✅ Line Bot API 初始化成功。")
except Exception as e:
    print(f"❌ Line Bot API 初始化失敗，請檢查 Token：{e}")
    exit()

# ---------------------------- LINE/Gemini 相關函式 ----------------------------

def get_gemini_response(prompt_text):
    """使用 Google Gemini API 取得內容。"""
    try:
        # Client() 會自動偵測並使用環境變數中的 GEMINI_API_KEY
        client = genai.Client() 
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

def send_stock_notification(user_id, message_data: Union[str, FlexMessage]):
    """
    發送訊息的通用函式，可接受 FlexMessage 物件或純文字。
    """
    global messaging_api # 使用全域變數

    try:
        if isinstance(message_data, str):
            messages_list = [TextMessage(text=message_data)]
            message_type = "純文字訊息"
        elif isinstance(message_data, FlexMessage):
            messages_list = [message_data]
            message_type = "Flex Message"
        else:
            print(f"【錯誤】不支援的訊息類型: {type(message_data)}")
            return

        push_message_request = PushMessageRequest(
            to=user_id,
            messages=messages_list
        )
        messaging_api.push_message(push_message_request) 
        print(f"✅ {message_type} 已成功發送給 {user_id}")
    except Exception as e:
        print(f"❌ 訊息發送錯誤: {e}")

def create_color_stock_flex(df_data: pd.DataFrame, date_to_check: str) -> FlexMessage:
    """
    根據篩選後的漲跌資料建立 Flex Message，並將漲跌設為紅色。
    """
    header_contents = [
        {
            "type": "text",
            "text": f"📈 {date_to_check} 漲勢股速報",
            "weight": "bold",
            "size": "md",
            "color": "#1DB446"
        }
    ]
    body_contents = []
    data_to_show = df_data.head(15) 

    for index, row in data_to_show.iterrows():
        change_text = row['漲跌(+/-)']
        # 漲幅用紅色
        color = "#FF0000" if change_text == '+' else "#000000" 

        content = {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {
                    "type": "text",
                    "text": f"{row['證券代號']} {row['證券名稱']}",
                    "flex": 3, "size": "sm", "color": "#333333", "align": "start"
                },
                {
                    "type": "text",
                    "text": f"{change_text}{row['漲跌價差']} ({row['收盤價']})",
                    "flex": 2, "size": "sm", "color": color, "align": "end", "weight": "bold"
                }
            ],
            "spacing": "sm", "margin": "sm"
        }
        body_contents.append(content)
        
    body_contents.append({"type": "separator", "margin": "md"})
    body_contents.append({
        "type": "text",
        "text": f"✅ 總共找到 {len(df_data)} 檔符合條件。",
        "size": "sm", "color": "#AAAAAA", "align": "center", "margin": "md"
    })
    flex_content = {
        "type": "bubble",
        "header": {"type": "box", "layout": "vertical", "contents": header_contents},
        "body": {"type": "box", "layout": "vertical", "contents": body_contents}
    }

    # 使用 V3 SDK 的正確傳入方式 (直接傳入字典)
    return FlexMessage(
        alt_text=f"{date_to_check} 漲勢股速報，共 {len(df_data)} 檔。",
        contents=flex_content
    )


# ---------------------------- TWSE 資料處理函式 ----------------------------

def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """讀取並清理 TWSE Big5 編碼的 CSV 資料。"""
    try:
        csv_data = StringIO(response_text)
        df = pd.read_csv(
            csv_data, 
            header=header_row, 
            skipinitialspace=True, 
            on_bad_lines='skip', 
            encoding='Big5' 
        )
        if not df.empty:
            df.columns = df.columns.str.strip()
        df = df.dropna(how='all')
        if not df.empty and df.iloc[-1].astype(str).str.contains('合計|總計|備註', na=False).any():
            df = df.iloc[:-1]
        return df
    except Exception as e:
        print(f"在讀取或清理 CSV 數據時發生錯誤: {e}")
        return None

def _fetch_twse_data(url: str) -> Optional[str]:
    """共用的輔助函式，用於發送 HTTP 請求並檢查狀態。"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # 由於 TWSE 可能有 SSL 問題，這裡使用 verify=False (但建議正式環境使用 verify=True)
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

def fetch_twse_mi_index20(target_date: str, file_path: Path) -> Optional[pd.DataFrame]:
    """抓取指定日期的 MI_INDEX20 報告 (收盤指數及成交量值資訊)。"""
    if not re.fullmatch(r'\d{8}', target_date): return None
    
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20"
    url = f"{base_url}?date={target_date}&response=csv"
    
    jutils.check_and_create_folder(file_path.parent) 
    
    print(f"嘗試抓取 MI_INDEX20 資料 ({target_date})...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None
    
    # 修正：表頭索引為 2 (TWSE 格式通常是第 3 行)
    df = _read_twse_csv(response_text, header_row=2) 

    if df is not None and '證券代號' in df.columns:
        df = df[df['證券代號'].astype(str).str.strip() != ''] 
        df.to_csv(file_path, index=False, encoding='utf-8-sig') 
        print(f"✅ {file_path.name} 儲存成功。")
        return df
    print(f"【注意】MI_INDEX20 報表無有效資料或欄位缺失。")
    return None

def check_date_in_csv(file_path: Path, date_to_check: str, date_column_name: str = '日期') -> bool:
    """檢查指定日期是否出現在 CSV 檔案的特定欄位中 (用於檢查休市日)。"""
    print(f"🔍 正在檢查假日檔案: {file_path.name}")
    print(f"目標日期: {date_to_check}")

    if not file_path.exists():
        print("【錯誤】假日檔案路徑不存在。")
        return False
        
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            return False

        is_present = df[date_column_name].isin([date_to_check]).any()
        
        if is_present:
            print(f"✅ 日期 '{date_to_check}' 已在檔案中找到！確認為休市日。")
            return True
        else:
            print(f"❌ 日期 '{date_to_check}' 未在檔案中找到。")
            return False
    except Exception as e:
        print(f"【錯誤】讀取或處理假日檔案時發生錯誤: {e}")
        return False

# ---------------------------- 主要運行邏輯 ----------------------------

def main_run():
    """主要的資料處理、篩選和訊息發送邏輯。"""
    
    # 1. 檢查是否為休市日
    is_holiday = check_date_in_csv(HOLIDAY_FILE_PATH, DATE_TO_CHECK_HOLIDAY, HOLIDAY_DATE_COLUMN)
    
    if is_holiday:
        report_msg = f"📅 {DATE_TO_CHECK_HOLIDAY} 股市為休市日，無交易資料。"
        print(report_msg)
        send_stock_notification(LINE_USER_ID, report_msg) 
        return

    # 2. 抓取今日資料
    df_raw = fetch_twse_mi_index20(TARGET_DATE, file_path)
    
    if df_raw is None or df_raw.empty:
        no_data_message = f"【注意】{DATE_TO_CHECK_HOLIDAY} 抓取資料失敗或無交易資料。"
        print(no_data_message)
        send_stock_notification(LINE_USER_ID, no_data_message)
        return
        
    # 3. 讀取並篩選資料
    try:
        # 讀取剛剛抓取的檔案 (確保編碼一致)
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        df['證券代號'] = df['證券代號'].astype(str).str.strip()
        
        # 篩選條件 1: 取得【證券代號】為4碼數字的廠商
        df_filtered = df[df['證券代號'].str.match(r'^\d{4}$', na=False)]
        
        # 篩選條件 2: 取得【漲跌(+/-)】欄位為【+】(上漲)
        df_filtered = df_filtered[df_filtered['漲跌(+/-)'].astype(str).str.strip() == '+']
        
        # 選擇需要的欄位
        result = df_filtered[['證券代號', '證券名稱' , '收盤價' ,'漲跌(+/-)' , '漲跌價差']].copy()
        
        # 4. 輸出結果與發送 LINE 訊息
        if not result.empty:
            print("--------------------")
            print(f"總共找到 {len(result)} 筆上漲資料。")

            # 建立並發送 Flex Message (帶有顏色)
            flex_message_object = create_color_stock_flex(result, DATE_TO_CHECK_HOLIDAY)
            send_stock_notification(LINE_USER_ID, flex_message_object)
        else:
            no_data_message = f"【篩選結果】{DATE_TO_CHECK_HOLIDAY} 未找到符合所有篩選條件（四碼數字且上漲）的資料。"
            print(no_data_message)
            send_stock_notification(LINE_USER_ID, no_data_message)

    except Exception as e:
        error_msg = f"❌ 篩選或處理資料時發生錯誤：{e}"
        print(error_msg)
        send_stock_notification(LINE_USER_ID, error_msg)


# ---------------------------- 排程運行 ----------------------------

schedule.clear()
print("---------------------------------------")

# 設置排程：每 5 秒檢查一次 (測試用)
schedule.every(5).seconds.do(main_run) 

print(f"⏰ 排程器啟動。每 5 秒執行 main_run 函式...")

# 無窮迴圈運行排程
while True:
    try:
        schedule.run_pending()
        time.sleep(1) 
    except KeyboardInterrupt:
        print("\n程式已手動中斷。")
        break
    except Exception as e:
        print(f"排程迴圈發生錯誤: {e}")
        time.sleep(5)