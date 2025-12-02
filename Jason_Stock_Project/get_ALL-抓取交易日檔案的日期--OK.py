import tkinter as tk
from tkinter import scrolledtext
import sys
import time
import threading
import requests
import pandas as pd
from typing import Optional, List, Any, Dict
from io import StringIO
from datetime import datetime, timedelta
import pathlib
import urllib3
import re # 引入正則表達式

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 設定與路徑 (全面使用 pathlib) ---
# 獲取當前文件所在的目錄
BASE_DIR = pathlib.Path(__file__).resolve().parent

# 交易日清單路徑 (用於決定所有資料源的抓取日期/月份範圍)
TRADING_DAY_CSV_PATH = BASE_DIR / "datas" / "processed" / "get_holidays" / f"trading_day_2021-2025.csv"

# 股票清單路徑 (新增，用於 STOCK_DAY 抓取)
STOCKS_ALL_CSV_PATH = BASE_DIR / "datas" / "raw" / "stocks_all.csv"

# STOCK_DAY 輸出基本路徑 (新增)
STOCK_DAY_OUTPUT_BASE_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"
STOCK_DAY_BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"


# --- 日報表 (每日摘要) 資料源配置 ---
# 格式: (URL基本路徑, 輸出目錄名, 檔案後綴, 清理欄位名, 表頭行索引, 額外URL參數)
DATA_SOURCES = [
    ("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX", "2_MI_INDEX", "_MI_INDEX_Sector", "指數", 2, "&type=ALLBUT0999"),
    ("https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d", "3_BWIBBU_d", "_BWIBBU_d_IndexReturn", "產業別", 1, None),
    ("https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU", "5_TWTASU", "_TWTASU_VolumePrice", "項目", 1, None),
    ("https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU", "6_BFIAMU", "_BFIAMU_DealerTrade", "自營商", 1, None),
    ("https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK", "7_FMTQIK", "_FMTQIK_BrokerVolume", "券商", 1, None),
    ("https://www.twse.com.tw/rwd/zh/fund/BFI82U", "8_BFI82U", "_BFI82U_3IParty_Day", "項目", 1, None),
    ("https://www.twse.com.tw/rwd/zh/fund/TWT43U", "9_TWT43U", "_TWT43U_ForeignTrade", "外資及陸資", 1, None),
    ("https://www.twse.com.tw/rwd/zh/fund/TWT44U", "10_TWT44U", "_TWT44U_InvestmentTrust", "投信", 1, None),
    ("https://www.twse.com.tw/rwd/zh/fund/T86", "11_T86", "_T86_InstitutionalTrades", "證券代號", 1, "&selectType=ALL"),
]


# ==========================================================
# 核心原子功能函式 (單一職責)
# ==========================================================

def ensure_output_directory_exists(path: pathlib.Path):
    """
    功能: 確保給定的目錄路徑存在。
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        print(f"✅ 已創建輸出目錄: {path}")
        
def get_latest_end_date() -> datetime.date:
    """功能: 根據現在時間 (21:00 前/後) 確定抓取的截止日期 (昨天/今天)。"""
    now = datetime.now()
    cutoff_time = datetime.strptime("21:00:00", "%H:%M:%S").time()
    
    if now.time() >= cutoff_time:
        return now.date()
    else:
        return (now - timedelta(days=1)).date()

def fetch_raw_data_from_url(url: str, is_stock_day: bool = False) -> Optional[str]:
    """
    功能: 嘗試從 TWSE 抓取資料，並返回原始文本。
    """
    try:
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Big5' # TWSE 數據大多使用 Big5 編碼
        
        # 檢查 TWSE 回傳內容是否為錯誤訊息
        if "很抱歉" in response.text or "查無相關資料" in response.text:
            return None
        
        # 日報表專用檢查
        if not is_stock_day and "查詢日期大於今日" in response.text:
            return None
            
        return response.text
        
    except requests.exceptions.HTTPError:
        # 對於 STOCK_DAY，404/400 等也視為無資料，返回 None 即可
        return None
    except requests.exceptions.RequestException:
        # 連線或 Requests 錯誤，返回 None
        return None
    except Exception:
        # 其他錯誤，返回 None
        return None
        
    return None

def parse_twse_raw_csv(response_text: str, header_index: int, cleanup_column: str) -> Optional[pd.DataFrame]:
    """
    功能: 將 TWSE 返回的文本解析為 Pandas DataFrame，並執行清理 (適用於日報表摘要)。
    """
    try:
        # 使用 StringIO 模擬檔案讀取，並指定編碼為 'utf-8-sig' 兼容處理
        data = StringIO(response_text)
        
        df = pd.read_csv(data, 
                         header=header_index, 
                         encoding='utf-8-sig', # 在解析時使用 utf-8-sig 處理 CSV 內容
                         skipinitialspace=True,
                         engine='python',
                         on_bad_lines='skip' 
        )
        
        if df.empty:
            return None
        
        df.columns = df.columns.str.strip() 
        df.dropna(axis=1, how='all', inplace=True)
        
        if cleanup_column in df.columns:
            # 移除清理欄位為空的行
            df = df[df[cleanup_column].astype(str).str.strip() != '']
            
        return df if not df.empty else None 

    except Exception:
        return None

def parse_twse_stock_day_csv(response_text: str, header_row: int = 1) -> Optional[pd.DataFrame]:
    """
    功能: 將 TWSE 返回的 STOCK_DAY 文本解析為 Pandas DataFrame，並執行清理。
    """
    try:
        data = StringIO(response_text)
        # STOCK_DAY 的 CSV 格式通常表頭在索引 1
        df = pd.read_csv(data, 
                         header=header_row, 
                         encoding='utf-8-sig', # 在解析時使用 utf-8-sig 處理 CSV 內容
                         skipinitialspace=True,
                         engine='python',
                         on_bad_lines='skip' 
        )
        if not df.empty:
            df.columns = df.columns.str.strip() # 清理欄位名稱
            df.dropna(axis=1, how='all', inplace=True) # 移除所有內容為空的欄位
            
            if df.empty:
                return None
            
            # 清理：移除日期為空的行 (通常是尾部的註解或空行)
            if '日期' in df.columns:
                 df = df[df['日期'].astype(str).str.strip() != '']
                
            return df if not df.empty else None

        return None

    except Exception:
        return None

def save_dataframe_to_csv(df: pd.DataFrame, file_path: pathlib.Path) -> bool:
    """
    功能: 將 DataFrame 儲存為 CSV 檔案 (Pathlib)。
    
    注意: 依照使用者要求，寫入編碼設定為 'big5'。
    """
    try:
        ensure_output_directory_exists(file_path.parent) 
        # 依照使用者要求，寫入編碼使用 'big5'
        df.to_csv(file_path, index=False, encoding='big5') 
        return True
    except Exception as e:
        print(f"❌ 資料儲存失敗: {e}")
        return False

# ==========================================================
# 資料清單獲取函式 (適用於所有任務)
# ==========================================================

def get_trading_dates_from_csv(csv_path: pathlib.Path) -> Optional[List[str]]:
    """
    功能: 讀取、處理、篩選交易日清單 (含多種編碼嘗試)。
    """
    # 依照使用者要求，嘗試多種編碼讀取
    encodings_to_try = ['utf-8-sig', 'cp950', 'big5', 'utf-8']
    df = None
    
    print(f"--- 嘗試讀取檔案 {csv_path.name} ---")
    
    if not csv_path.exists():
        print(f"錯誤: 找不到檔案 {csv_path}，請確認路徑。")
        return None
        
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            print(f"【成功】檔案使用 '{encoding}' 編碼成功讀取。")
            break 
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"讀取檔案 {csv_path.name} 時發生非編碼錯誤: {e}")
            return None 
            
    if df is None or df.empty:
        print(f"錯誤: 檔案 {csv_path.name} 無法用任何預設編碼讀取或內容為空。")
        return None
        
    try:
        # 假設日期在第一欄
        date_column = df.columns[0]
        df['dt_obj'] = pd.to_datetime(df[date_column].astype(str).str.strip(), errors='coerce')
        df.dropna(subset=['dt_obj'], inplace=True)
        processed_dates = df['dt_obj'].dt.strftime('%Y%m%d').unique().tolist()
        all_dates_list = sorted(processed_dates)
        
        if not all_dates_list:
            print(f"錯誤: 檔案 {csv_path.name} 中找不到任何有效日期數據。")
            return None
            
        return all_dates_list
            
    except Exception as e:
        print(f"錯誤: 處理檔案 {csv_path.name} 內容時發生錯誤 (如欄位缺失): {e}")
        return None

def get_filtered_daily_date_list(all_dates_list: List[str]) -> Optional[List[str]]:
    """
    功能: 從所有交易日中，篩選出符合抓取截止時間的每日清單。
    """
    if not all_dates_list:
        return None
        
    end_date = get_latest_end_date()
    
    print(f"【時間判斷】截止日為 {end_date.strftime('%Y/%m/%d')}。")

    start_date_str = all_dates_list[0]
    end_date_str = end_date.strftime("%Y%m%d")
    
    filtered_dates = [
        date_str for date_str in all_dates_list 
        if start_date_str <= date_str <= end_date_str
    ]

    if not filtered_dates:
        print(f"警告: 在範圍 [{start_date_str} - {end_date_str}] 內找不到任何日期。")
        return []
        
    print(f"--- 最終每日日期清單 (共 {len(filtered_dates)} 天) ---")
    return filtered_dates

def get_month_list_from_start_to_end(all_dates_list: List[str]) -> Optional[List[str]]:
    """
    功能: 根據交易日清單，生成從起始月份到截止月份的所有月份清單 (YYYYMMDD，以該月第一天表示)。
    """
    if not all_dates_list:
        print("錯誤: 交易日清單為空。")
        return None

    start_date_str = all_dates_list[0]
    end_date = get_latest_end_date()
    
    # 轉換為 datetime object
    start_dt = datetime.strptime(start_date_str, "%Y%m%d")
    end_dt = datetime(end_date.year, end_date.month, end_date.day)

    month_list = []
    current_dt = datetime(start_dt.year, start_dt.month, 1)

    while current_dt <= end_dt:
        # TWSE STOCK_DAY API 只需要 YYYYMMDD 格式，通常選擇該月的第一天作為代表日期
        month_list.append(current_dt.strftime("%Y%m%d")) 
        
        # 移動到下個月
        if current_dt.month == 12:
            current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
        else:
            current_dt = current_dt.replace(month=current_dt.month + 1)

    print(f"--- 最終月份清單 (共 {len(month_list)} 個月) ---")
    print(f"起始月份: {month_list[0][:6]}, 截止月份: {month_list[-1][:6]}")
    return month_list

def get_stock_list(file_path: pathlib.Path) -> Optional[List[str]]:
    """功能: 從 stocks_all.csv 讀取所有股票代號。"""
    try:
        # 嘗試使用 utf-8-sig 讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        # 假設股票代號在第一欄
        stock_list = df.iloc[:, 0].astype(str).str.strip().tolist()
        
        # 簡單過濾：只保留長度為 4~6 的數字串
        filtered_stocks = [s for s in stock_list if re.fullmatch(r'\d{4,6}', s)]
        
        if not filtered_stocks:
            print(f"錯誤: 檔案 {file_path.name} 中找不到任何有效的股票代號。")
            return None
        
        print(f"--- 成功讀取 {len(filtered_stocks)} 個股票代號 ---")
        return filtered_stocks
    except FileNotFoundError:
        print(f"錯誤: 找不到股票清單檔案 {file_path}，請確認路徑。")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理股票清單檔案 {file_path.name} 時發生錯誤: {e}")
        return None

# ==========================================================
# 流程控制函式 (Orchestrator) - 負責調度和重試
# ==========================================================

def orchestrate_daily_summary_tasks(date_list: List[str], stop_event: threading.Event):
    """
    功能: 負責處理所有每日報表摘要的批次抓取流程。
    """
    print("\n========================================================")
    print("--- 開始批次抓取 TWSE 每日報表摘要 (Daily Summaries) ---")
    print("========================================================\n")
    
    for source in DATA_SOURCES:
        if stop_event.is_set():
            break
            
        base_url, dir_name, file_suffix, cleanup_column, header_index, url_suffix_fragment = source
        
        current_raw_data_dir = BASE_DIR / "datas" / "raw" / dir_name
        ensure_output_directory_exists(current_raw_data_dir)
        
        print(f"\n--- 🔄 正在處理資料源: {dir_name} (共 {len(date_list)} 天) ---")

        for target_date in date_list:
            if stop_event.is_set():
                break
                
            max_attempts = 4
            file_path = current_raw_data_dir / f"{target_date}{file_suffix}.csv"
            
            # 檢查檔案是否已存在，存在則跳過
            if file_path.exists():
                continue 
            
            is_successful = False
            
            for attempt in range(1, max_attempts + 1):
                if stop_event.is_set():
                    break

                # 構建 URL 
                url_parts = [base_url, "?"]
                url_parts.append(f"date={target_date}")
                if url_suffix_fragment:
                    url_parts.append(url_suffix_fragment)
                url_parts.append("&response=csv") 
                url = "".join(url_parts)
                
                # 呼叫原子功能函式: 抓取原始文本
                response_text = fetch_raw_data_from_url(url, is_stock_day=False)
                
                df = None
                if response_text is not None:
                    # 呼叫原子功能函式: 解析為 DataFrame
                    df = parse_twse_raw_csv(response_text, header_index, cleanup_column)
                
                if df is not None and not df.empty:
                    # 呼叫原子功能函式: 儲存 CSV
                    if save_dataframe_to_csv(df, file_path):
                        print(f" ✅ {target_date} / {dir_name} 資料已完成。")
                        is_successful = True
                        break 
                elif response_text is None:
                    # 網站返回無資料 (如假日)，可能是正常情況，退出重試
                    break
                
                # 失敗處理 (只有在抓取到數據但解析失敗，或連線有問題時才重試)
                if not is_successful and not stop_event.is_set() and attempt < max_attempts:
                    delay_seconds = attempt * 5 
                    time.sleep(delay_seconds)
                elif not is_successful and not stop_event.is_set() and attempt == max_attempts:
                    print(f"❌ {target_date} / {dir_name} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此日期。")
                    break
                    
            if not stop_event.is_set():
                time.sleep(2) # 抓取間隔
                
    if stop_event.is_set():
        print("\n*** Daily Summaries 任務被使用者強制停止 ***")
    else:
        print("\n=== Daily Summaries 任務處理完畢 ===")


def fetch_twse_stock_day_single(target_date: str, stock_no: str) -> Optional[pd.DataFrame]:
    """
    功能: 抓取指定月份和股票代號的 STOCK_DAY 報告。
    行為更新: 若網路上有資料，直接寫入並覆蓋當月檔案 (不再因檔案存在而跳過)。
    返回值:
      - DataFrame: 成功抓取並儲存
      - None: 抓取失敗或無資料
    """
    # 輸出路徑 (包含股票代號)
    output_dir = STOCK_DAY_OUTPUT_BASE_DIR / stock_no
    month_str = target_date[:6]
    filename = output_dir / f"{month_str}_{stock_no}_STOCK_DAY.csv"
    ensure_output_directory_exists(output_dir)

    # 構造 URL
    url = f"{STOCK_DAY_BASE_URL}?date={target_date}&stockNo={stock_no}&response=csv"

    # 1. 抓取數據
    response_text = fetch_raw_data_from_url(url, is_stock_day=True)
    if response_text is None:
        # 無數據或請求失敗
        return None

    # 2. 解析數據
    df = parse_twse_stock_day_csv(response_text, header_row=1)
    if df is None or df.empty:
        # 解析失敗或該月無交易資料
        return None

    # 3. 儲存資料（若檔案已存在，pandas 會直接覆寫）
    saved = save_dataframe_to_csv(df, filename)
    if saved:
        return df
    else:
        return None
    
def orchestrate_stock_day_task(month_list: List[str], stock_list: List[str], stop_event: threading.Event):
    """
    功能: 負責處理 TWSE STOCK_DAY (股票日線圖) 的批次抓取流程。
    行為更新: 會嘗試抓取每一個月的資料，若成功會寫入並覆蓋當月檔案；不再以「已存在」跳過。
    """
    print("\n========================================================")
    print("--- 開始批次抓取 TWSE STOCK_DAY (股票日線圖) ---")
    print("========================================================\n")
    
    total_tasks = len(month_list) * len(stock_list)
    tasks_successful = 0
    tasks_overwritten = 0
    tasks_new = 0
    tasks_failed = 0
    
    for stock_no in stock_list:
        if stop_event.is_set():
            break
            
        print(f"\n--- 🔄 開始處理股票代號: {stock_no} (共 {len(month_list)} 個月) ---")
        
        for target_date in month_list:
            if stop_event.is_set():
                break
                
            month_str = target_date[:6]
            is_done = False
            
            max_attempts = 4
            for attempt in range(1, max_attempts + 1):
                if stop_event.is_set():
                    break
                    
                df_result = fetch_twse_stock_day_single(target_date, stock_no)
                
                if isinstance(df_result, pd.DataFrame):
                    filename = (STOCK_DAY_OUTPUT_BASE_DIR / stock_no / f"{month_str}_{stock_no}_STOCK_DAY.csv")
                    # 判斷是否為覆寫還是新建（檔案在寫入前是否存在）
                    if filename.exists():
                        print(f" ✅ {stock_no} | {month_str} 資料已覆寫 (overwrite)。")
                        tasks_overwritten += 1
                    else:
                        print(f" ✅ {stock_no} | {month_str} 資料儲存成功 (new).")
                        tasks_new += 1
                    tasks_successful += 1
                    is_done = True
                    break
                elif df_result is None:
                    # 無資料或請求失敗，直接退出重試 (通常表示該月無交易或連線被拒)
                    tasks_failed += 1
                    break 
                
                # 失敗處理 (只有在連線有問題時才重試)
                if not is_done and not stop_event.is_set() and attempt < max_attempts:
                    delay_seconds = attempt * 5 
                    time.sleep(delay_seconds)
                elif not is_done and not stop_event.is_set() and attempt == max_attempts:
                    print(f"❌ {stock_no} | {month_str} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此月份。")
                    tasks_failed += 1
                    break
            
            if not stop_event.is_set():
                time.sleep(2) # 抓取間隔
    
    if stop_event.is_set():
        print("\n*** STOCK_DAY 任務被使用者強制停止 ***")
    else:
        print("\n--- 🏁 STOCK_DAY 批次抓取結束 ---")
        print(f"成功儲存任務數 (含覆寫與新建): {tasks_successful}")
        print(f"  - 覆寫檔案數: {tasks_overwritten}")
        print(f"  - 新建檔案數: {tasks_new}")
        print(f"失敗任務數: {tasks_failed}")
        print(f"總任務數: {total_tasks}")


def orchestrate_main_crawling(stop_event: threading.Event):
    """
    功能: 協調所有爬蟲任務的頂層函式。
    """
    
    print("--- 🏁 啟動 TWSE 總爬蟲任務 ---")
    
    # 1. 獲取核心日期清單 (所有任務的日期範圍都以此為準)
    all_trading_dates = get_trading_dates_from_csv(TRADING_DAY_CSV_PATH)
    if not all_trading_dates:
        print("致命錯誤：無法取得交易日清單，終止所有爬蟲任務。")
        return

    # --- 執行每日報表摘要 (Daily Summaries) ---
    daily_date_list = get_filtered_daily_date_list(all_trading_dates)
    if daily_date_list:
        orchestrate_daily_summary_tasks(daily_date_list, stop_event)
    
    if stop_event.is_set():
        return

    # --- 執行股票日線圖 (STOCK_DAY) ---
    month_list = get_month_list_from_start_to_end(all_trading_dates)
    stock_list = get_stock_list(STOCKS_ALL_CSV_PATH)
    
    if month_list and stock_list:
        orchestrate_stock_day_task(month_list, stock_list, stop_event)

    print("\n=== 總爬蟲任務批次處理完畢 ===")


# ==========================================================
# GUI 介面設定
# ==========================================================

class TextRedirector:
    """功能: 將 stdout 導向 Tkinter 的 ScrolledText widget"""
    def __init__(self, widget):
        self.widget = widget
        self.widget.tag_configure("stdout", foreground="black")

    def write(self, string):
        self.widget.insert(tk.END, string, "stdout")
        self.widget.see(tk.END)
        self.widget.update_idletasks() 

    def flush(self):
        pass

def _set_start_button_normal(start_btn: tk.Button):
    """GUI 輔助: 將開始按鈕設為 NORMAL 狀態"""
    start_btn.config(state=tk.NORMAL, text="開始執行")

def _set_stop_button_disabled(stop_btn: tk.Button):
    """GUI 輔助: 將停止按鈕設為 DISABLED 狀態"""
    stop_btn.config(state=tk.DISABLED)

def _revert_stop_button_text(button: tk.Button):
    """
    GUI 輔助: 將停止按鈕的文字從 '停止中...' 恢復為 '停止'。
    此函式用於實現 3 秒後文字還原。
    """
    button.config(text="停止")

def _reset_gui_buttons(start_btn: tk.Button, stop_btn: tk.Button):
    """功能: 更新 GUI 按鈕狀態 (確保在主執行緒上執行)"""
    root = start_btn.winfo_toplevel()
    root.after(0, _set_start_button_normal, start_btn)
    root.after(0, _set_stop_button_disabled, stop_btn)
    root.after(0, _revert_stop_button_text, stop_btn) # 確保結束時文字也恢復

# 定義一個全域變數來保存排程 ID
SCHEDULE_JOB_ID = None

def _schedule_next_run(root, start_btn, stop_event, schedule_label_var, on_start_func):
    """
    功能: 任務結束後，延遲 1 秒重新啟動排程。
    """
    schedule_daily_run(root, start_btn, stop_event, schedule_label_var, on_start_func)

def run_crawling_thread(start_btn: tk.Button, stop_btn: tk.Button, stop_event: threading.Event, on_start_func, schedule_label_var):
    """
    功能: 作為執行緒目標的主邏輯，負責調度所有爬蟲任務。
    """
    
    # 執行主流程控制
    orchestrate_main_crawling(stop_event)
    
    # 重置 GUI 狀態
    _reset_gui_buttons(start_btn, stop_btn)
    
    # 任務結束後，重新啟動排程
    root = start_btn.winfo_toplevel()
    root.after(1000, _schedule_next_run, root, start_btn, stop_event, schedule_label_var, on_start_func) 


def _reschedule_next_day(root, btn_start, stop_event, schedule_label, on_start_func):
    """
    功能: 排程動作結束後，重新設定下一天的排程。
    """
    schedule_daily_run(root, btn_start, stop_event, schedule_label, on_start_func)


def schedule_daily_run(root, btn_start, stop_event, schedule_label, on_start_func):
    """
    功能: 計算並設定下一次每日 21:00 執行爬蟲的時間。
    """
    global SCHEDULE_JOB_ID
    
    if SCHEDULE_JOB_ID:
        try:
            root.after_cancel(SCHEDULE_JOB_ID)
        except:
            pass 
            
    TARGET_HOUR = 21
    TARGET_MINUTE = 0
    TARGET_SECOND = 0
    
    now = datetime.now()
    target_time_today = now.replace(hour=TARGET_HOUR, minute=TARGET_MINUTE, second=TARGET_SECOND, microsecond=0)
    
    if now > target_time_today:
        next_run = target_time_today + timedelta(days=1)
    else:
        next_run = target_time_today
        
    delay_seconds = (next_run - now).total_seconds()
    delay_ms = max(1000, int(delay_seconds * 1000))

    schedule_label.set(f"下次排程執行時間: {next_run.strftime('%Y/%m/%d %H:%M:%S')} (手動點擊可立即執行)")
    print(f"\n[系統訊息] 下次排程抓取時間已設定為: {next_run.strftime('%Y/%m/%d %H:%M:%S')}")

    def scheduled_action():
        """排程觸發時執行的動作 (定義在內部以方便存取閉包變數)"""
        global SCHEDULE_JOB_ID
        SCHEDULE_JOB_ID = None 
        
        if btn_start['state'] == tk.NORMAL:
            print("\n[系統訊息] 🚨 排程時間到達 (21:00)，自動開始抓取任務...")
            # 呼叫傳入的 on_start 函式引用
            on_start_func()
        else:
            print("\n[系統訊息] 🚨 排程時間到達，但因任務正在執行，故跳過本次自動啟動。")
            
        # 無論是否啟動，都必須重新排程下一輪運行
        if btn_start['state'] == tk.NORMAL:
              root.after(1000, _reschedule_next_day, root, btn_start, stop_event, schedule_label, on_start_func)

    SCHEDULE_JOB_ID = root.after(delay_ms, scheduled_action)


def run_gui():
    """功能: 啟動 Tkinter GUI 應用程式。"""
    
    root = tk.Tk()
    root.title("Python TWSE 多源爬蟲 (整合 STOCK_DAY) - 固定每日 21:00 排程抓取")
    
    # --- 視窗居中計算 ---
    window_width = 800
    window_height = 600
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    center_x = int((screen_width / 2) - (window_width / 2))
    center_y = int((screen_height / 2) - (window_height / 2))
    root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    # --- 視窗居中計算結束 ---


    # 文字輸出區
    scrolled_text = scrolledtext.ScrolledText(root, width=80, height=20, font=("Consolas", 10))
    scrolled_text.pack(padx=10, pady=10, expand=True, fill='both')

    # 重導向 stdout
    sys.stdout = TextRedirector(scrolled_text)
    
    # 排程狀態顯示
    schedule_status = tk.StringVar()
    schedule_label = tk.Label(root, textvariable=schedule_status, fg="#2E86C1", font=("Arial", 10, "bold"))
    schedule_label.pack(pady=(0, 5)) 

    # 控制區
    btn_frame = tk.Frame(root)
    btn_frame.pack(pady=10)

    # 停止事件
    stop_event = threading.Event()
    
    # 預先宣告按鈕 
    btn_start = None
    btn_stop = None

    def on_exit():
        """功能: 處理應用程式關閉。"""
        # 1. 設置停止事件，通知爬蟲執行緒停止
        stop_event.set()
        # 2. 取消排程
        global SCHEDULE_JOB_ID
        if SCHEDULE_JOB_ID:
            try:
                root.after_cancel(SCHEDULE_JOB_ID)
            except Exception:
                pass
        # 3. 關閉視窗
        root.destroy()
        
    def on_start():
        """功能: 手動啟動爬蟲任務 (定義在內部以存取按鈕變數)"""
        global SCHEDULE_JOB_ID
        
        if SCHEDULE_JOB_ID:
            try:
                # 取消當前排程，因為手動啟動後會重新設定下一次排程
                root.after_cancel(SCHEDULE_JOB_ID)
                SCHEDULE_JOB_ID = None
                print("\n[系統訊息] 手動啟動，已取消當前排程。")
            except:
                pass 
                
        stop_event.clear()
        btn_start.config(state=tk.DISABLED, text="執行中...")
        btn_stop.config(state=tk.NORMAL, text="停止") # 確保啟動時文字是「停止」
        
        # 啟動執行緒來執行爬蟲邏輯 (傳遞 on_start 函式引用，因為它是核心啟動邏輯)
        t = threading.Thread(target=run_crawling_thread, args=(btn_start, btn_stop, stop_event, on_start, schedule_status))
        t.daemon = True
        t.start()

    def on_stop():
        """功能: 手動停止爬蟲任務"""
        stop_event.set()
        # 立即禁用按鈕並更改文字
        btn_stop.config(state=tk.DISABLED, text="停止中...")
        
        # 3 秒後將文字還原回「停止」
        root.after(3000, _revert_stop_button_text, btn_stop)


    btn_start = tk.Button(btn_frame, text="開始執行", command=on_start, 
                         bg="#4CAF50", fg="white", font=("Arial", 12, "bold"), padx=15)
    btn_start.pack(side=tk.LEFT, padx=10)

    btn_stop = tk.Button(btn_frame, text="停止", command=on_stop, 
                         bg="#F44336", fg="white", font=("Arial", 12, "bold"), padx=15, state=tk.DISABLED)
    btn_stop.pack(side=tk.LEFT, padx=10)
    
    btn_exit = tk.Button(btn_frame, text="離開程式", command=on_exit,
                         bg="#607D8B", fg="white", font=("Arial", 12, "bold"), padx=15)
    btn_exit.pack(side=tk.LEFT, padx=10)
    
    # 首次啟動排程
    # 傳入 on_start 函式引用，讓排程器能在時間到達時觸發它
    schedule_daily_run(root, btn_start, stop_event, schedule_status, on_start)


    # 顯示所有資料的預計存儲父目錄
    initial_dir = BASE_DIR / "datas" / "raw"
    print(f"系統準備就緒。")
    print(f"交易日清單路徑: {TRADING_DAY_CSV_PATH}")
    print(f"股票清單路徑: {STOCKS_ALL_CSV_PATH}")
    print(f"所有日報表摘要將存於子目錄下: {initial_dir} (2_MI_INDEX ~ 11_T86)")
    print(f"所有股票日線圖將存於子目錄下: {STOCK_DAY_OUTPUT_BASE_DIR} / {{股票代號}}")

    root.protocol("WM_DELETE_WINDOW", on_exit) 
    
    root.mainloop()

if __name__ == '__main__':
    # 確保主輸出目錄存在
    ensure_output_directory_exists(BASE_DIR / "datas" / "raw") 
    run_gui()