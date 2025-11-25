import os
import re
import time
import requests
import pandas as pd
from typing import Optional, List, Dict
from io import StringIO
from datetime import datetime, timedelta
import pathlib
import sys

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 全域設定與路徑 ---

CODE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS_ALL_CSV = os.path.join(CODE_DIR, "datas", "raw", "stocks_all.csv")
# 交易日清單檔案路徑 (僅用於輔助判斷今日是否為交易日，不再用於生成歷史範圍)
CSV_FILE_PATH = os.path.join(CODE_DIR, "datas", "processed", "get_holidays", "trading_day_2021-2025.csv")


# --- 輔助函數 ---
# 從交易日清單中找到當前日期的【前一個交易日】。
def _get_previous_trading_day(file_path: str, current_date: datetime.date) -> Optional[datetime.date]:
    
    try:
        # 讀取交易日清單
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        date_column = df.columns[0]
        
        # 1. 統一轉換日期格式為 YYYYMMDD 字串
        trading_days_ymd = []
        for date_str in df[date_column].astype(str).str.strip().tolist():
            try:
                # 嘗試使用 YYYY/MM/DD 格式解析 (根據您的錯誤訊息)
                dt_obj = datetime.strptime(date_str, "%Y/%m/%d").date()
            except ValueError:
                try:
                    # 嘗試使用 YYYYMMDD 格式解析 (作為備用或標準格式)
                    dt_obj = datetime.strptime(date_str, "%Y%m%d").date()
                except ValueError:
                    # 忽略無法識別的格式
                    continue 
            
            # 將所有日期統一轉換為 YYYYMMDD 字串格式進行比較
            trading_days_ymd.append(dt_obj.strftime("%Y%m%d"))
            
        all_trading_days = sorted(list(set(trading_days_ymd)))
        
        # 2. 進行比較
        current_date_str = current_date.strftime("%Y%m%d")
        
        # 找到所有比今天日期小的交易日
        previous_trading_days = [
            d for d in all_trading_days if d < current_date_str
        ]
        
        if previous_trading_days:
            # 返回其中最大的一個 (即最近的一個交易日)
            # 因為 previous_trading_days 已經是排序好的 YYYYMMDD 字串列表
            return datetime.strptime(previous_trading_days[-1], "%Y%m%d").date()
        else:
            print("⚠️ 錯誤：交易日清單中找不到前一個交易日。")
            return None

    except FileNotFoundError:
        print(f"致命錯誤：找不到交易日清單檔案 {file_path}。")
        return None
    except Exception as e:
        # 捕捉其他可能的錯誤，並印出，但希望在內部處理掉 ValueError
        print(f"致命錯誤：處理交易日清單時發生錯誤: {e}")
        return None
    
# 檢查並建立所需的【資料夾】
def _check_folder_and_create(filepath: str):
    
    pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)

# 根據 21:00 基準判斷目標日期
def _get_target_date_and_month() -> Dict[str, Optional[str]]:
    """
    根據 21:00 基準判斷目標日期：
    1. 21:00 之後：抓取今天 (實際日期) 的資料。
    2. 21:00 之前：抓取【前一個交易日】的資料。
    """
    now = datetime.now()
    cutoff_time = datetime.strptime("21:00:00", "%H:%M:%S").time()
    
    if now.time() >= cutoff_time:
        # 情況 1: 21:00 之後 -> 抓取今天 (實際日期)
        target_date_dt = now.date()
        print(f"【日期判斷】{now.strftime('%H:%M:%S')} 晚於 21:00，目標日為今天 ({target_date_dt.strftime('%Y/%m/%d')})。")

    else:
        # 情況 2: 21:00 之前 -> 抓取前一個交易日
        
        # 先找到【日曆上的昨天】
        yesterday_dt = (now - timedelta(days=1)).date()
        
        # 然後從交易日清單中找到最近的交易日
        previous_trading_day_dt = _get_previous_trading_day(CSV_FILE_PATH, now.date())

        if previous_trading_day_dt:
            target_date_dt = previous_trading_day_dt
            print(f"【日期判斷】{now.strftime('%H:%M:%S')} 早於 21:00，目標日為前一個交易日 ({target_date_dt.strftime('%Y/%m/%d')})。")
        else:
            print("❌ 錯誤：無法確定前一個交易日，所有日報任務將跳過。")
            return {
                "daily_date": None,
                "monthly_date": None
            }

    # 紀錄開始的時間          
    start_time = datetime.now().strftime('%H:%M:%S')
 
    # 針對日報：
    final_daily_date = target_date_dt.strftime("%Y%m%d")
    
    # 針對 STOCK_DAY：只需要目標日期所在月份的代表日期 (YYYYMM01)
    current_month_date = target_date_dt.strftime("%Y%m") + "01"

    print(f"【最終目標】日報抓取日期: {final_daily_date}")
    print(f"【最終目標】STOCK_DAY月份: {current_month_date[:6]}")

    return {
        "daily_date": final_daily_date,
        "monthly_date": current_month_date,
        "start_time": start_time
    }

# 從 stocks_all.csv 讀取股票清單，並依據【市場別】欄位篩選出「上市」公司。
def get_stock_list(file_path: str) -> Optional[List[str]]:

    try:
        # 讀取整個 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 1. 尋找【市場別】欄位
        # 嘗試從欄位名稱中找出包含 "市場別"、"類別" 或 "市場" 的欄位
        market_col = None
        for col in df.columns:
            if "市場別" in col or "市場" in col or "類別" in col:
                market_col = col
                break
        
        if market_col is None:
            # 警告：如果找不到市場別欄位，則退回到只抓取 4 位數字的代號（避免抓取全部）
            print("警告：找不到包含 '市場別' 或 '市場' 字樣的欄位，將退回僅篩選 4 位數字代號。")
            
            stock_list = df.iloc[:, 0].astype(str).str.strip().tolist()
            filtered_stocks = [
                s for s in stock_list 
                if re.fullmatch(r'^\d{4}$', s)
            ]
            
        else:
            # 2. 找到市場別欄位，進行篩選
            
            # 清理市場別欄位的字串，並篩選出包含「上市」字樣的行
            df_listed = df[
                df[market_col].astype(str).str.strip().str.contains("上市", na=False)
            ].copy() # 使用 copy 避免 SettingWithCopyWarning
            
            # 3. 取得第一欄的股票代號
            # 假設股票代號是第一欄 (索引 0)
            stock_list = df_listed.iloc[:, 0].astype(str).str.strip().tolist()
            
            # 額外檢查：確保代號是有效的數字格式（通常是 4 位純數字）
            filtered_stocks = [s for s in stock_list if re.fullmatch(r'\d{4,6}', s)]
            
        
        if not filtered_stocks:
            print("錯誤: 依據「市場別」篩選後，找不到任何符合條件的上市公司代號。")
            return None
            
        print(f"--- 成功依據「市場別」篩選，讀取 {len(filtered_stocks)} 個上市公司代號 ---")
        print(filtered_stocks)
        return filtered_stocks
    except pd.errors.EmptyDataError:
        print(f"錯誤: 股票清單檔案 {file_path} 為空。")
        return None
    except Exception as e:
        print(f"錯誤: 讀取或處理股票清單檔案 {file_path} 時發生錯誤: {e}")
        return None

# 將 TWSE 返回的文本解析為 Pandas DataFrame。
def _read_twse_csv(response_text: str, header_row: int = 1, first_col_name: Optional[str] = None) -> Optional[pd.DataFrame]:
    
    try:
        data = StringIO(response_text)
        df = pd.read_csv(data, 
                         header=header_row, 
                         encoding='utf-8-sig', 
                         skipinitialspace=True,
                         engine='python',
                         on_bad_lines='skip' 
        )
        if not df.empty:
            df.columns = df.columns.str.strip()
            df.dropna(axis=1, how='all', inplace=True)
            
            if df.empty: return None
            
            if first_col_name in df.columns:
                 df = df[df[first_col_name].astype(str).str.strip() != '']
                
            return df
        return None
    except Exception as e:
        return None


# --- 通用日報抓取主函數 (僅抓取當日) ---
def fetch_single_daily_report(
    target_date: str, 
    base_url: str, 
    output_folder_num: str,
    output_filename_suffix: str,
    url_params: str = "",
    first_col_name: Optional[str] = None,
    header_row: int = 1
):
    """
    抓取單日報表數據的通用函數，具備檔案存在即跳過的機制。
    """
    
    OUTPUT_DIR = os.path.join(CODE_DIR, "datas", "raw", output_folder_num)
    filename = os.path.join(OUTPUT_DIR, f"{target_date}{output_filename_suffix}.csv")
    _check_folder_and_create(filename)

    print(f"\n--- 🚀 處理 {output_folder_num} ({target_date}) ---")

    # 1. 檢查檔案是否已存在
    if os.path.exists(filename):
        print(f"  ℹ️ {target_date} 資料已存在，跳過。")
        return # 跳過，不執行延遲

    # 2. 檔案不存在，開始執行抓取和重試
    is_successful = False
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        
        url = f"{base_url}?date={target_date}{url_params}&response=csv"
        
        # 執行抓取
        response_text = _fetch_twse_data(url)
        df = None
        if response_text is not None:
            df = _read_twse_csv(response_text, header_row=header_row, first_col_name=first_col_name)

        if df is not None:
            # 成功儲存
            try:
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"  ✅ {target_date} 資料儲存成功。")
                is_successful = True
                break
            except Exception as e:
                print(f"❌ {target_date} 資料儲存失敗: {e}")
                break

        # 失敗處理
        if attempt < max_attempts:
            delay_seconds = attempt * 5 
            print(f"🚨 抓取失敗 (第 {attempt} 次)。將在 {delay_seconds} 秒後重試...")
            time.sleep(delay_seconds)
        else:
            print(f"❌ {target_date} 資料經過 {max_attempts} 次嘗試後仍然失敗。")
            break
    
    # 3. 只有在進行了網路抓取或重試之後，才需要等待 2 秒
    if is_successful or attempt == max_attempts:
        time.sleep(2)

def _fetch_twse_data(url: str) -> Optional[str]:
    """嘗試從 TWSE 抓取資料，並返回原始文本。"""
    try:
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        response.encoding = 'Big5'
        
        if "很抱歉" in response.text or "查無相關資料" in response.text:
            return None
        
        return response.text
        
    except requests.exceptions.HTTPError:
        return None 
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        
    return None

# --- 任務 1: STOCK_DAY 獨立處理 (當月/覆蓋) ---
# 抓取當月所有股票的 STOCK_DAY 資料，並直接覆蓋檔案。
def fetch_twse_stock_day_single_month(month_date: str, stock_list: List[str]):

    print(f"\n--- 🚀 開始 STOCK_DAY 抓取 ({month_date[:6]}) (將直接覆蓋) ---")
    
    OUTPUT_BASE_DIR = os.path.join(CODE_DIR, "datas", "raw", "1_STOCK_DAY")
    BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
    
    tasks_successful = 0
    tasks_failed = 0
    
    for stock_no in stock_list:
        output_dir = os.path.join(OUTPUT_BASE_DIR, stock_no)
        month_str = month_date[:6]
        filename = os.path.join(output_dir, f"{month_str}_{stock_no}_STOCK_DAY.csv") 
        _check_folder_and_create(filename)
        
        is_successful = False
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            
            url = f"{BASE_URL}?date={month_date}&stockNo={stock_no}&response=csv"
            
            # 1. 抓取數據
            response_text = _fetch_twse_data(url)
            df = None
            if response_text is not None:
                df = _read_twse_csv(response_text, header_row=1, first_col_name='日期') 
            
            if df is not None and not df.empty:
                # 2. 儲存資料 (直接覆蓋)
                try:
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"  ✅ {stock_no} | {month_str} 資料已覆蓋儲存。")
                    tasks_successful += 1
                    is_successful = True
                    break
                except Exception as e:
                    print(f"❌ {stock_no} | {month_str} 資料儲存失敗: {e}")
                    tasks_failed += 1
                    break
            
            # 抓取失敗 (df is None)
            if attempt < max_attempts:
                delay_seconds = attempt * 5 
                print(f"🚨 {stock_no} | {month_str} 抓取失敗。將在 {delay_seconds} 秒後重試...")
                time.sleep(delay_seconds)
            else:
                print(f"❌ {stock_no} | {month_str} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此股票。")
                tasks_failed += 1
                break
                
        # 3. 每次嘗試網路請求後，等待 2 秒 (無論成功或失敗)
        if is_successful or attempt == max_attempts:
            time.sleep(2)
            
    print(f"\n--- 🏁 STOCK_DAY 抓取結束。成功覆蓋: {tasks_successful}, 失敗: {tasks_failed} ---")

# --- 主執行函數 ---

def main():
    
    # 1. 獲取單一目標日期和月份
    target_info = _get_target_date_and_month()
    daily_date = target_info["daily_date"]
    monthly_date = target_info["monthly_date"]
    start_time = target_info["start_time"]
    
    # 2. 獲取【股票代號】清單
    stock_list = get_stock_list(STOCKS_ALL_CSV)
    
    if daily_date is None:
        print("\n--------------------------------------")
        print("⚠️ 由於無法確定目標交易日，日報任務已跳過。")
        print("--------------------------------------")
    else:
        # --- A. 處理單日報表 (共 10 個任務) ---
        
        # 1. 集中交易市場統計資訊 (MI_INDEX)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX", "2_MI_INDEX", "_MI_INDEX_Sector", 
                                first_col_name="項目", header_row=2)
        
        # 2. 集中市場各類股成交量值 (BWIBBU_d)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d", "3_BWIBBU_d", "_BWIBBU_d_IndexReturn",
                                first_col_name="產業別", header_row=1)
                                
        # 3. 股票/指數期貨成交量值 (TWTASU)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU", "5_TWTASU", "_TWTASU_VolumePrice",
                                first_col_name="項目", header_row=1)
                                
        # 4. 自營商買賣金額 (BFIAMU)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU", "6_BFIAMU", "_BFIAMU_DealerTrade",
                                first_col_name="自營商", header_row=1)
                                
        # 5. 券商成交量值總表 (FMTQIK)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK", "7_FMTQIK", "_FMTQIK_BrokerVolume",
                                first_col_name="券商", header_row=1)
                                
        # 6. 三大法人買賣超金額 (BFI82U) - 注意 URL 參數結構不同
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/fund/BFI82U", "8_BFI82U", "_BFI82U_3IParty_Day",
                                url_params="&type=day&dayDate",
                                first_col_name="項目", header_row=1)
                                
        # 7. 外資及陸資買賣超彙總表 (TWT43U)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/fund/TWT43U", "9_TWT43U", "_TWT43U_ForeignTrade",
                                first_col_name="外資及陸資", header_row=1)
                                
        # 8. 投信買賣超彙總表 (TWT44U)
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/fund/TWT44U", "10_TWT44U", "_TWT44U_InvestmentTrust",
                                first_col_name="投信", header_row=1)
                                
        # 9. 三大法人買賣超統計 (T86) - ALL
        fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/fund/T86", "11_T86", "_T86_InstitutionalTrades",
                                url_params="&selectType=ALL",
                                first_col_name="證券代號", header_row=1)

        # 10. 融資融券餘額 (TWT92U)
        #fetch_single_daily_report(daily_date, "https://www.twse.com.tw/rwd/zh/marginTrading/TWT92U", "4_TWT92U", "_TWT92U_Margin",
        #                        first_col_name="股票代號", header_row=1)
                                
    # --- B. 處理 STOCK_DAY (任務 1 - 當月覆蓋) ---
    if stock_list and monthly_date:
        fetch_twse_stock_day_single_month(monthly_date, stock_list)
    elif not stock_list:
        print("警告：無法取得股票清單 (stocks_all.csv)，跳過 STOCK_DAY 抓取。")
    elif not monthly_date:
        print("警告：無法取得目標月份，跳過 STOCK_DAY 抓取。")
        
    print("\n======================================")
    print("✅ 所有 TWSE 數據抓取任務已完成。")
    print(f"【開始時間】{start_time} ")
    print(f"【完成時間】{datetime.now().strftime('%H:%M:%S')} ")
    print("======================================")
    
if __name__ == "__main__":
    main()