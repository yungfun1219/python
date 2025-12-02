import tkinter as tk
from tkinter import scrolledtext
import sys
import time
import threading
import requests
import pandas as pd
import urllib3
import pathlib
import os  # <--- 1. 修正點：移到最上方全域匯入
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Line Bot 相關
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
from linebot.v3.messaging import TextMessage, PushMessageRequest

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================================
# 參數與路徑設定 (使用 pathlib)
# ==========================================================
BASE_DIR = pathlib.Path(__file__).resolve().parent
LIST_FILE_PATH = BASE_DIR / "datas" / "raw" / "stocks_all.csv"
RAW_DATA_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"
ENV_PATH = BASE_DIR / "line_API.env"

MIN_START_DATE_STR = '2025/11/01'
CODE_COL = '有價證券代號'
LIST_DATE_COL = '上市日'

# 確保輸出目錄存在
if not RAW_DATA_DIR.exists():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================================
# 單一功能函式庫
# ==========================================================

def setup_line_api(env_file_path):
    """讀取環境變數並初始化 Line API"""
    # 2. 修正點：函式內部乾淨，直接使用全域的 os
    if not env_file_path.exists():
        print(f"【警告】找不到設定檔: {env_file_path}")
        return None, None
    
    load_dotenv(env_file_path)
    
    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    user_id = os.getenv("LINE_USER_ID")

    if token and user_id:
        try:
            config = Configuration(access_token=token)
            api_client = ApiClient(config)
            messaging_api = MessagingApi(api_client)
            return messaging_api, user_id
        except Exception as e:
            print(f"【錯誤】Line API 初始化失敗: {e}")
            return None, None
    else:
        print("【提示】未讀取到 Token 或 User ID，Line 通知功能將停用。")
        return None, None

def send_line_message(api, user_id, text):
    """發送 Line 訊息"""
    if api and user_id:
        try:
            req = PushMessageRequest(to=user_id, messages=[TextMessage(text=text)])
            api.push_message(req)
            print(f"Line 訊息已發送: {text}")
        except Exception as e:
            print(f"【錯誤】Line 發送失敗: {e}")

def load_stock_codes(csv_path):
    """讀取 CSV 並回傳股票代號列表"""
    print(f"--- 讀取清單: {csv_path.name} ---")
    if not csv_path.exists():
        print(f"【錯誤】找不到檔案: {csv_path}")
        return []
    
    try:
        df = pd.read_csv(csv_path, dtype={CODE_COL: str})
        if CODE_COL not in df.columns:
            print(f"【錯誤】CSV 中找不到欄位: {CODE_COL}")
            return []
            
        codes = df[CODE_COL].str.strip().dropna().tolist()
        print(f"【成功】取得 {len(codes)} 筆代號")
        return codes
    except Exception as e:
        print(f"【錯誤】讀取清單失敗: {e}")
        return []

def get_stock_info(csv_path, target_code):
    """從清單中查詢單一股票的 名稱 與 上市日"""
    try:
        df = pd.read_csv(csv_path, dtype={CODE_COL: str})
        df[CODE_COL] = df[CODE_COL].astype(str).str.strip()
        row = df[df[CODE_COL] == target_code]
        
        if row.empty:
            return "Unknown", None
        
        name = row.iloc[0].get('有價證券名稱', 'Unknown')
        list_date = row.iloc[0].get(LIST_DATE_COL, None)
        return name, list_date
    except Exception:
        return "Unknown", None

def calculate_crawl_months(listing_date_str):
    """計算需要爬取的月份清單 (YYYYMM 格式)"""
    try:
        if not listing_date_str:
            return []
        
        # 處理日期格式，確保是 YYYY/MM/DD
        listing_date_str = listing_date_str.replace('-', '/')
        
        actual_date = datetime.strptime(listing_date_str, '%Y/%m/%d').date()
        min_date = datetime.strptime(MIN_START_DATE_STR, '%Y/%m/%d').date()
        
        start_date = max(actual_date, min_date)
        today = date.today()
        
        month_list = []
        curr = start_date
        # 若 start_date 是 2023/01/15，我們還是要把 202301 加入
        # 簡單做法：將 curr 設為該月1號
        curr = curr.replace(day=1)
        
        while curr <= today:
            month_list.append(curr.strftime('%Y%m'))
            curr += relativedelta(months=1)
            
        return month_list
    except ValueError:
        print(f"   日期格式錯誤: {listing_date_str}")
        return []

def convert_roc_date(roc_date_str):
    """轉換民國日期字串為西元 (輔助函式)"""
    try:
        sep = '/' if '/' in roc_date_str else '-'
        y, m, d = roc_date_str.split(sep)
        return f"{int(y) + 1911}/{m.zfill(2)}/{d.zfill(2)}"
    except:
        return roc_date_str

def fetch_monthly_data(stock_code, year_month):
    """爬取指定月份的資料，回傳 DataFrame 或 None"""
    print(f"   正在爬取: {year_month}...", end=" ")
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={year_month}01&stockNo={stock_code}"
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'}
    
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=10)
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        if '很抱歉，沒有符合條件的資料' in soup.text:
            print("無資料")
            return None
            
        table = soup.find('table')
        if not table:
            print("表格異常")
            return None
            
        # 解析表格
        thead = table.find('thead')
        tbody = table.find('tbody')
        
        if not thead or not tbody:
            print("表格結構不完整")
            return None

        columns = [th.text.strip() for th in thead.find_all('th')][1:]
        rows = []
        for tr in tbody.find_all('tr'):
            tds = [td.text.strip() for td in tr.find_all('td')]
            if tds:
                tds[0] = convert_roc_date(tds[0]) # 呼叫轉換日期
                rows.append(tds)
        
        print("OK")
        return pd.DataFrame(rows, columns=columns)
        
    except Exception as e:
        print(f"失敗 ({e})")
        return None

def remove_current_month_data(csv_path):
    """從 CSV 檔案中刪除當前月份的資料"""
    if not csv_path.exists():
        return
    
    current_ym = date.today().strftime('%Y%m')
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        if '日期' not in df.columns: return

        # 建立暫時欄位判斷月份
        df['temp_date'] = pd.to_datetime(df['日期'], errors='coerce')
        df['temp_ym'] = df['temp_date'].dt.strftime('%Y%m')
        
        # 篩選非當月
        new_df = df[df['temp_ym'] != current_ym].drop(columns=['temp_date', 'temp_ym'])
        
        # 如果有刪除資料，才回存
        if len(new_df) < len(df):
            new_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"   已清除 {csv_path.name} 當月舊資料，準備重新爬取。")
    except Exception as e:
        print(f"【錯誤】清除當月資料失敗: {e}")

def append_to_csv(df, csv_path):
    """將 DataFrame 寫入 CSV"""
    if not csv_path.exists():
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print("   -> 建立新檔案")
    else:
        try:
            # 簡單檢查重複：讀取檔案最後一筆日期
            existing_df = pd.read_csv(csv_path, encoding='utf-8-sig')
            if not existing_df.empty:
                last_date_in_file = existing_df['日期'].iloc[-1]
                first_date_in_new = df['日期'].iloc[0]
                
                # 如果新資料的第一天已經存在於檔案中，則跳過
                if first_date_in_new in existing_df['日期'].values:
                    print("   -> 資料可能重複，跳過寫入")
                    return

            df.to_csv(csv_path, mode='a', index=False, header=False, encoding='utf-8-sig')
            print("   -> 資料已追加")
        except Exception as e:
            print(f"【錯誤】寫入失敗: {e}")

def clean_and_sort_csv(csv_path):
    """最終整理：排序並去重"""
    if not csv_path.exists(): return
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        if '日期' not in df.columns: return
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').drop_duplicates(subset=['日期'], keep='last')
        df['日期'] = df['日期'].dt.strftime('%Y/%m/%d')
        
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"【錯誤】排序整理失敗: {e}")

# ==========================================================
# 核心邏輯 (Thread Target)
# ==========================================================

def run_crawling_thread(start_btn, stop_btn, stop_event):
    """
    主邏輯控制中心
    """
    print("="*40)
    print("      股票爬蟲系統啟動 (修正版)      ")
    print("="*40)

    # 1. Line 初始化
    line_api, line_user_id = setup_line_api(ENV_PATH)

    # 2. 取得股票清單
    codes = load_stock_codes(LIST_FILE_PATH)
    total = len(codes)
    
    if total == 0:
        print("無代號可處理，結束。")
        _reset_gui_buttons(start_btn, stop_btn)
        return

    # 3. 逐一處理股票
    for idx, code in enumerate(codes, 1):
        if stop_event.is_set():
            print("\n*** 使用者強制停止 ***")
            break

        # 3-1. 取得個股資訊
        name, listing_date = get_stock_info(LIST_FILE_PATH, code)
        print(f"\n[{idx}/{total}] 代號: {code} ({name})")

        if not listing_date:
            print("   無法取得上市日，跳過。")
            continue

        # 3-2. 計算需爬取的月份
        months_to_crawl = calculate_crawl_months(listing_date)
        if not months_to_crawl:
            print("   無需爬取月份 (日期範圍外)。")
            continue

        # 3-3. 定義存檔路徑
        file_name = f"{code}_{name}_stock.csv"
        save_path = RAW_DATA_DIR / file_name

        # 3-4. 清除檔案中的當月資料
        remove_current_month_data(save_path)

        # 3-5. 月份迴圈爬取
        for ym in months_to_crawl:
            if stop_event.is_set(): 
                break
            
            df = fetch_monthly_data(code, ym)
            
            if df is not None:
                append_to_csv(df, save_path)
            
            time.sleep(3) # 月份間隔

        # 3-6. 完成該股後，整理檔案
        clean_and_sort_csv(save_path)
        
        # 股票間的休息
        if not stop_event.is_set():
            print("   等待 5 秒進入下一檔...")
            time.sleep(5)

    # 4. 結束通知
    msg = "股票爬蟲任務已完成" if not stop_event.is_set() else "股票爬蟲任務已被中斷"
    print(f"\n{msg}")
    if line_api:
        send_line_message(line_api, line_user_id, msg)

    _reset_gui_buttons(start_btn, stop_btn)

def _reset_gui_buttons(start_btn, stop_btn):
    """更新 GUI 按鈕狀態"""
    root = start_btn.winfo_toplevel()
    root.after(0, lambda: start_btn.config(state=tk.NORMAL, text="開始執行"))
    root.after(0, lambda: stop_btn.config(state=tk.DISABLED))
# ==========================================================
# GUI 介面設定
# ==========================================================

class TextRedirector:
    def __init__(self, widget):
        self.widget = widget
        self.widget.tag_configure("stdout", foreground="black")

    def write(self, string):
        self.widget.insert(tk.END, string, "stdout")
        self.widget.see(tk.END)
        self.widget.update_idletasks()

    def flush(self):
        pass

def run_gui():
    root = tk.Tk()
    root.title("Python Stock Crawler (V2)")
    
    # --- 視窗居中計算 ---
    window_width = 650
    window_height = 550
    
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    
    center_x = int((screen_width / 2) - (window_width / 2))
    center_y = int((screen_height / 2) - (window_height / 2))
    
    root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    # --- 視窗居中計算結束 ---


    # 文字輸出區
    scrolled_text = scrolledtext.ScrolledText(root, width=70, height=20, font=("Consolas", 10))
    scrolled_text.pack(padx=10, pady=10, expand=True, fill='both')

    # 重導向
    sys.stdout = TextRedirector(scrolled_text)

    # 控制區
    btn_frame = tk.Frame(root)
    btn_frame.pack(pady=10)

    stop_event = threading.Event()

    def on_start():
        stop_event.clear()
        btn_start.config(state=tk.DISABLED, text="執行中...")
        btn_stop.config(state=tk.NORMAL)
        
        t = threading.Thread(target=run_crawling_thread, args=(btn_start, btn_stop, stop_event))
        t.daemon = True
        t.start()

    def on_stop():
        stop_event.set()
        btn_stop.config(state=tk.DISABLED, text="停止中...")

    # 新增的離開程式函式
    def on_exit():
        print("\n[系統訊息] 程式準備退出...")
        # 1. 發出停止信號給工作執行緒
        stop_event.set()
        # 2. 銷毀主視窗
        root.destroy()


    btn_start = tk.Button(btn_frame, text="開始執行", command=on_start, 
                          bg="#4CAF50", fg="white", font=("Arial", 12, "bold"), padx=15)
    btn_start.pack(side=tk.LEFT, padx=10)

    btn_stop = tk.Button(btn_frame, text="停止", command=on_stop, 
                         bg="#F44336", fg="white", font=("Arial", 12, "bold"), padx=15, state=tk.DISABLED)
    btn_stop.pack(side=tk.LEFT, padx=10)
    
    # 新增的離開按鈕
    btn_exit = tk.Button(btn_frame, text="離開程式", command=on_exit,
                         bg="#607D8B", fg="white", font=("Arial", 12, "bold"), padx=15)
    btn_exit.pack(side=tk.LEFT, padx=10) # 放在最右邊

    print(f"系統準備就緒。")
    print(f"讀取清單路徑: {LIST_FILE_PATH}")
    print(f"資料存檔路徑: {RAW_DATA_DIR}")
    
    # 確保關閉視窗時也能觸發 on_exit 邏輯
    root.protocol("WM_DELETE_WINDOW", on_exit) 
    
    root.mainloop()

if __name__ == '__main__':
    run_gui()