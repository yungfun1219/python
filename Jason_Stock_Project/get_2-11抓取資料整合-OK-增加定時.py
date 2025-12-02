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

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 設定與路徑 (全面使用 pathlib) ---
# 獲取當前文件所在的目錄
BASE_DIR = pathlib.Path(__file__).resolve().parent

# 交易日清單路徑
TRADING_DAY_CSV_PATH = BASE_DIR / "datas" / "processed" / "get_holidays" / f"trading_day_2021-2025.csv"

# --- 多重資料源配置 ---
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
        

def get_filtered_date_list(csv_path: pathlib.Path) -> Optional[List[str]]:
    """
    功能: 讀取、處理、篩選交易日清單 (含多種編碼嘗試)。
    """
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
        date_column = df.columns[0]
        df['dt_obj'] = pd.to_datetime(df[date_column].astype(str).str.strip(), errors='coerce')
        df.dropna(subset=['dt_obj'], inplace=True)
        processed_dates = df['dt_obj'].dt.strftime('%Y%m%d').unique().tolist()
        all_dates_list = sorted(processed_dates)
        
        if not all_dates_list:
            print(f"錯誤: 檔案 {csv_path.name} 中找不到任何有效日期數據。")
            return None
            
        now = datetime.now()
        cutoff_time = datetime.strptime("21:00:00", "%H:%M:%S").time()
        
        # 21:00 前只抓取到前一日，21:00 後可以抓取當日
        end_date = now.date() if now.time() >= cutoff_time else (now - timedelta(days=1)).date()
            
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
            
        print(f"--- 最終日期清單 (共 {len(filtered_dates)} 天) ---")
        return filtered_dates
            
    except Exception as e:
        print(f"錯誤: 處理檔案 {csv_path.name} 內容時發生錯誤 (如欄位缺失): {e}")
        return None

def fetch_raw_data_from_url(url: str) -> Optional[str]:
    """
    功能: 嘗試從 TWSE 抓取資料，並返回原始文本。
    """
    try:
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Big5'
        
        if "查詢日期大於今日" in response.text or "很抱歉" in response.text or "查無相關資料" in response.text:
            return None
        
        return response.text
        
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

def parse_twse_raw_csv(response_text: str, header_index: int, cleanup_column: str) -> Optional[pd.DataFrame]:
    """
    功能: 將 TWSE 返回的文本解析為 Pandas DataFrame，並執行清理。
    """
    try:
        data = StringIO(response_text)
        
        df = pd.read_csv(data, 
                         header=header_index, 
                         encoding='utf-8-sig', 
                         skipinitialspace=True,
                         engine='python',
                         on_bad_lines='skip' 
        )
        
        if df.empty:
            return None
        
        df.columns = df.columns.str.strip() 
        df.dropna(axis=1, how='all', inplace=True)
        
        if cleanup_column in df.columns:
            df = df[df[cleanup_column].astype(str).str.strip() != '']
            
        return df if not df.empty else None 

    except Exception:
        # 即使解析失敗，也保持流程平坦，只返回 None
        return None

def save_dataframe_to_csv(df: pd.DataFrame, file_path: pathlib.Path) -> bool:
    """
    功能: 將 DataFrame 儲存為 CSV 檔案 (Pathlib)。
    """
    try:
        ensure_output_directory_exists(file_path.parent) 
        df.to_csv(file_path, index=False, encoding='utf-8-sig') 
        print(f"  ✅ 資料儲存成功: {file_path.name}")
        return True
    except Exception as e:
        print(f"❌ 資料儲存失敗: {e}")
        return False

# ==========================================================
# 流程控制函式 (Orchestrator) - 負責調度和重試
# ==========================================================

def orchestrate_crawling_task(date_list: List[str], stop_event: threading.Event):
    """
    流程調度函式。負責多資料源和多日期的批次處理流程。
    """
    print("--- 開始批次抓取 TWSE 多源資料 ---")
    
    for source in DATA_SOURCES:
        if stop_event.is_set():
            break
            
        base_url, dir_name, file_suffix, cleanup_column, header_index, url_suffix_fragment = source
        
        current_raw_data_dir = BASE_DIR / "datas" / "raw" / dir_name
        ensure_output_directory_exists(current_raw_data_dir)
        
        print(f"\n========================================================")
        print(f"✅ 正在處理資料源: {dir_name} ({base_url}) (共 {len(date_list)} 天)")
        print(f"========================================================\n")

        for target_date in date_list:
            if stop_event.is_set():
                break
                
            max_attempts = 4
            file_path = current_raw_data_dir / f"{target_date}{file_suffix}.csv"
            
            if file_path.exists():
                print(f"  ℹ️ {target_date} 資料已存在 ({dir_name})，跳過抓取。")
                continue 
            
            is_successful = False
            
            for attempt in range(1, max_attempts + 1):
                if stop_event.is_set():
                    break

                print(f"  -> 嘗試抓取 {target_date} / {dir_name} (第 {attempt} 次)...")
                
                # 構建 URL (流程扁平化，將 URL 構建邏輯放在此處)
                url_parts = [base_url, "?"]
                url_parts.append(f"date={target_date}")
                if url_suffix_fragment:
                    url_parts.append(url_suffix_fragment)
                url_parts.append("&response=csv") 
                url = "".join(url_parts)
                
                # 呼叫原子功能函式
                response_text = fetch_raw_data_from_url(url)
                
                df = None
                if response_text is not None:
                    # 呼叫原子功能函式
                    df = parse_twse_raw_csv(response_text, header_index, cleanup_column)
                
                if df is not None and not df.empty:
                    # 呼叫原子功能函式
                    if save_dataframe_to_csv(df, file_path):
                        print(f"🌟 {target_date} / {dir_name} 資料已完成。")
                        is_successful = True
                        break 
                elif response_text is None:
                    print(f"  ⚠️ {target_date} / {dir_name} 網站返回無資料，跳過。")
                    break
                else:
                    pass
                
                # 失敗處理
                if not is_successful and not stop_event.is_set() and attempt < max_attempts:
                    delay_seconds = attempt * 5 
                    print(f"🚨 {target_date} / {dir_name} 抓取或解析失敗。將在 {delay_seconds} 秒後重試...")
                    time.sleep(delay_seconds)
                elif not is_successful and not stop_event.is_set() and attempt == max_attempts:
                    print(f"❌ {target_date} / {dir_name} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此日期。")
                    break
                
            if (is_successful or attempt == max_attempts or response_text is None) and not stop_event.is_set():
                print("等待 2 秒後，準備處理下一個日期...")
                time.sleep(2)
            
    if stop_event.is_set():
        print("\n*** 使用者強制停止 ***")
    else:
        print("\n=== 爬蟲任務批次處理完畢 ===")


# ==========================================================
# GUI 介面設定 (移除所有 lambda)
# ==========================================================

class TextRedirector:
    """將 stdout 導向 Tkinter 的 ScrolledText widget"""
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
    """更新 GUI 按鈕狀態 (確保在主執行緒上執行)"""
    root = start_btn.winfo_toplevel()
    root.after(0, _set_start_button_normal, start_btn)
    root.after(0, _set_stop_button_disabled, stop_btn)
    root.after(0, _revert_stop_button_text, stop_btn) # 確保結束時文字也恢復

# 定義一個全域變數來保存排程 ID
SCHEDULE_JOB_ID = None

# 命名函式作為 run_crawling_thread 結束後的重新排程動作
def _schedule_next_run(root, start_btn, stop_event, schedule_label_var, on_start_func):
    """
    功能: 任務結束後，延遲 1 秒重新啟動排程。
    """
    schedule_daily_run(root, start_btn, stop_event, schedule_label_var, on_start_func)

def run_crawling_thread(start_btn: tk.Button, stop_btn: tk.Button, stop_event: threading.Event, on_start_func, schedule_label_var):
    """
    作為執行緒目標的主邏輯，負責調度爬蟲任務。
    """
    
    # 1. 執行日期清單生成 (呼叫原子功能)
    final_date_list = get_filtered_date_list(TRADING_DAY_CSV_PATH)
    
    if final_date_list:
        # 2. 執行主流程控制 (呼叫流程控制函式)
        orchestrate_crawling_task(final_date_list, stop_event)
    else:
        print("沒有可供處理的日期清單，程式結束。")
    
    # 3. 重置 GUI 狀態
    _reset_gui_buttons(start_btn, stop_btn)
    
    # 4. 任務結束後，重新啟動排程
    root = start_btn.winfo_toplevel()
    root.after(1000, _schedule_next_run, root, start_btn, stop_event, schedule_label_var, on_start_func) 


# 命名函式作為 scheduled_action 結束後的重新排程動作
def _reschedule_next_day(root, btn_start, stop_event, schedule_label, on_start_func):
    """
    功能: 排程動作結束後，重新設定下一天的排程。
    """
    schedule_daily_run(root, btn_start, stop_event, schedule_label, on_start_func)


def schedule_daily_run(root, btn_start, stop_event, schedule_label, on_start_func):
    """
    計算並設定下一次每日 21:00 執行爬蟲的時間。
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
            print("\n[系統訊息] 🚨 排程時間到達 (15:00)，自動開始抓取任務...")
            # 呼叫傳入的 on_start 函式引用
            on_start_func()
        else:
            print("\n[系統訊息] 🚨 排程時間到達，但因任務正在執行，故跳過本次自動啟動。")
            
        # 無論是否啟動，都必須重新排程下一輪運行
        if btn_start['state'] == tk.NORMAL:
             root.after(1000, _reschedule_next_day, root, btn_start, stop_event, schedule_label, on_start_func)

    SCHEDULE_JOB_ID = root.after(delay_ms, scheduled_action)


def run_gui():
    """啟動 Tkinter GUI 應用程式。"""
    
    root = tk.Tk()
    root.title("Python TWSE 多源爬蟲 (固定每日 21:00 排程抓取)")
    
    # --- 視窗居中計算 ---
    window_width = 750
    window_height = 550
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
        """處理應用程式關閉。"""
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
        """手動啟動爬蟲任務 (定義在內部以存取按鈕變數)"""
        global SCHEDULE_JOB_ID
        
        if SCHEDULE_JOB_ID:
            try:
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
        """手動停止爬蟲任務"""
        stop_event.set()
        # 立即禁用按鈕並更改文字
        btn_stop.config(state=tk.DISABLED, text="停止中...")
        
        # 3 秒後將文字還原回「停止」
        # 使用 root.after 實現延遲
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
    schedule_daily_run(root, btn_start, stop_event, schedule_status, on_start)


    # 顯示所有資料的預計存儲父目錄
    initial_dir = BASE_DIR / "datas" / "raw"
    print(f"系統準備就緒。")
    print(f"交易日清單路徑: {TRADING_DAY_CSV_PATH}")
    print(f"所有資料將存於子目錄下: {initial_dir}")
    
    root.protocol("WM_DELETE_WINDOW", on_exit) 
    
    root.mainloop()

if __name__ == '__main__':
    ensure_output_directory_exists(BASE_DIR / "datas" / "raw")
    run_gui()