import requests
import pandas as pd
from io import StringIO
import urllib3
import re
from datetime import date, datetime
from typing import Optional, Tuple, List
from pathlib import Path
import os
import utils.jason_utils as jutils
import get_stocks_company_all 
import time
import pathlib

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================================
# 參數設定  --- 配置 (Configuration) ---
# ==========================================================
LOG_FETCH_DATE_FILENAME = "last_get_date.log" # 定義記錄上次成功抓取日期的日誌檔案名稱
SUMMARY_LOG_FILENAME_PREFIX = "fetch_summary" # 定義摘要日誌檔案前綴

def get_past_dates_in_yyyymmdd(file_path, date_column_name='日期'):
    """
    讀取 CSV 檔案，篩選出在今天或今天之前的所有日期，並以 YYYYMMDD 字串格式返回。

    Args:
        file_path (str): CSV 檔案的路徑。
        date_column_name (str): CSV 中包含日期的欄位名稱。預設為 'Date'。

    Returns:
        list: 包含所有過去日期的 YYYYMMDD 格式字串列表，如果出錯則返回空列表。
    """
    try:
        # 1. 讀取 CSV 檔案
        df = pd.read_csv(file_path)

        # 2. 確保日期欄位是 datetime 格式
        # errors='coerce' 會將無法解析的值設為 NaT
        df[date_column_name] = pd.to_datetime(df[date_column_name], errors='coerce')

        # 3. 獲取今天的日期 (只取年月日部分)
        # 今天的日期為 2025-11-01
        today = pd.to_datetime(datetime.now().date()) 
        
        # 4. 篩選出今天之前 (即 <= 今天) 的日期資料
        # 篩選條件是：日期欄位值 <= 今天的日期
        past_dates_df = df[df[date_column_name] <= today]

        # 5. 移除日期為 NaT 的列
        past_dates_df = past_dates_df.dropna(subset=[date_column_name])
        
        # 6. 排序 (可選，通常日期資料按時間順序排列較好)
        past_dates_df = past_dates_df.sort_values(by=date_column_name)
        
        # 7. **【關鍵】格式化並返回日期列表**
        # 使用 .dt.strftime('%Y%m%d') 將 datetime 物件轉換為 YYYYMMDD 格式的字串
        yyyymmdd_list = past_dates_df[date_column_name].dt.strftime('%Y%m%d').tolist()
        
        return yyyymmdd_list

    except FileNotFoundError:
        print(f"錯誤：找不到檔案在路徑：{file_path}")
        return []
    except KeyError:
        print(f"錯誤：CSV 檔案中找不到名為 '{date_column_name}' 的日期欄位。")
        return []
    except Exception as e:
        print(f"發生其他錯誤：{e}")
        return []

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

# 共用的輔助函式，用於處理 TWSE 的 Big5 編碼和 Pandas 讀取邏輯。
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

# 將所有報告的抓取結果摘要寫入日誌檔案，並同時列印到控制台。
def log_summary_results(results: List[Tuple[str, Optional[pd.DataFrame]]], fetch_date: str, summary_filename_prefix: str = SUMMARY_LOG_FILENAME_PREFIX):
    BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    OUTPUT_DIR = BASE_DIR / "datas" / "logs"

    # 確保日誌資料夾存在
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    log_file_name = f"{summary_filename_prefix}_{fetch_date}.log"
    filename_new = OUTPUT_DIR / log_file_name 
    
    # 建立摘要內容字串
    summary_lines = []
    
    header = "\n" + "="*50 + "\n"
    header += f"--- {fetch_date} 報告抓取結果摘要 ---\n"
    header += "="*50
    summary_lines.append(header)
    
    success_count = 0
    fail_count = 0

    for name, df in results:
        if df is not None:
            line = f"\n[🟢 {name} (成功)] 數據筆數: {len(df)}"
            success_count += 1
        else:
            line = f"[🔴 {name} (失敗)] 無數據或抓取錯誤。"
            fail_count += 1
        summary_lines.append(line)

    footer = "\n" + "="*50
    footer += f"\n總結：成功 {success_count} 個報告, 失敗 {fail_count} 個報告。"
    footer += "\n所有成功抓取的 CSV 檔案已儲存至對應的 'datas/raw' 子資料夾中。"
    footer += "\n--- 日誌記錄結束 ---\n"
    
    summary_lines.append(footer)
    
    log_content = "\n".join(summary_lines)

    # 寫入日誌檔案
    try:
        with open(filename_new, 'w', encoding='utf-8') as f:
            f.write(log_content)
        
        # 同時列印到控制台
        print(log_content)
        print(f"[日誌] 成功將摘要結果寫入檔案：{filename_new}")
    except Exception as e:
        print(f"❌ 寫入摘要日誌檔案發生錯誤: {e}")

# --- 10 大 TWSE 報告抓取函式 (分頁與個股) ---
def fetch_twse_stock_day(target_date: str, stock_no: str) -> Optional[pd.DataFrame]:
    """
    (1/10) 抓取指定日期和股票代號的 STOCK_DAY 報告 (每日成交資訊)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
    url = f"{base_url}?date={target_date}&stockNo={stock_no}&response=csv"

    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "1_STOCK_DAY")
    filename = OUTPUT_DIR + f"\\{target_date}_{stock_no}_STOCK_DAY.csv"

    check_folder_and_create(filename)

    print(f"嘗試抓取 (1/10) {stock_no} STOCK_DAY 資料...")
    response_text = _fetch_twse_data(url)
    
    if response_text is None: return None
    
    df = _read_twse_csv(response_text, header_row=1)
    if df is not None and '日期' in df.columns:
        df = df[df['日期'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (1/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_mi_index(target_date: str) -> Optional[pd.DataFrame]:
    """
    (2/10) 抓取指定日期的 MI_INDEX 報告 (所有類股成交統計)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
    url = f"{base_url}?date={target_date}&type=ALLBUT0999&response=csv"
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "2_MI_INDEX")
    filename = OUTPUT_DIR + f"\\{target_date}_MI_INDEX_Sector.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (2/10) MI_INDEX (類股統計) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # MI_INDEX 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '指數' in df.columns:
        df = df[df['指數'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (2/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_bwibbu_d(target_date: str) -> Optional[pd.DataFrame]:
    """
    (3/10) 抓取指定日期的 BWIBBU_d 報告 (發行量加權股價指數類股日成交量值及報酬率)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
    url = f"{base_url}?date={target_date}&selectType=ALL&response=csv"
    
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "3_BWIBBU_d")
    filename = OUTPUT_DIR + f"\\{target_date}_BWIBBU_d_IndexReturn.csv"

    check_folder_and_create(filename)
        
    print(f"嘗試抓取 (3/10) BWIBBU_d (類股報酬率) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # BWIBBU_d 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '證券代號' in df.columns:
        df = df[df['證券代號'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (3/10) {filename} 儲存成功。")
        return df
    return None

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

def fetch_twse_twtasu(target_date: str) -> Optional[pd.DataFrame]:
    """
    (5/10) 抓取指定日期的 TWTASU 報告 (每日總成交量值與平均股價)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU
    
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU"
    url = f"{base_url}?date={target_date}&response=csv"
    
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "5_TWTASU")
    filename = OUTPUT_DIR + f"\\{target_date}_TWTASU_VolumePrice.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (5/10) TWTASU (總量值) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # TWTASU 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=2)
   
    if df is not None and '證券名稱' in df.columns: 
        df = df[df['證券名稱'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (5/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_bfiamu(target_date: str) -> Optional[pd.DataFrame]:
    """
    (6/10) 抓取指定日期的 BFIAMU 報告 (自營商買賣超彙總表)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU
    
    **修正:** 表頭索引改為 3 (原為 2)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU"
    url = f"{base_url}?date={target_date}&response=csv"
    
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "6_BFIAMU")
    filename = OUTPUT_DIR + f"\\{target_date}_BFIAMU_DealerTrade.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (6/10) BFIAMU (自營商買賣超) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # BFIAMU 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '分類指數名稱' in df.columns:
        df = df.dropna(how='all') 
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (6/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_fmtqik(target_date: str) -> Optional[pd.DataFrame]:
    """
    (7/10) 抓取指定日期的 FMTQIK 報告 (每日券商成交量值總表)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK
    
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
    url = f"{base_url}?date={target_date}&response=csv"
    
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "7_FMTQIK")
    filename = OUTPUT_DIR + f"\\{target_date}_FMTQIK_BrokerVolume.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (7/10) FMTQIK (券商成交總表) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # FMTQIK 報表的表頭在索引 2
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '日期' in df.columns:
        df = df[df['日期'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (7/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_bfi82u(target_date: str) -> Optional[pd.DataFrame]:
    """
    (8/10) 抓取指定日期的 BFI82U 報告 (三大法人買賣超彙總表 - 日)。
    URL: https://www.twse.com.tw/rwd/zh/fund/BFI82U
    
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U"
    # 只使用 dayDate 進行日期參數模組化
    url = f"{base_url}?type=day&dayDate={target_date}&response=csv"
        
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "8_BFI82U")
    filename = OUTPUT_DIR + f"\\{target_date}_BFI82U_3IParty_Day.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (8/10) BFI82U (三大法人日報) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # BFI82U 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '單位名稱' in df.columns:
        df = df[df['單位名稱'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (8/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_twt43u(target_date: str) -> Optional[pd.DataFrame]:
    """
    (9/10) 抓取指定日期的 TWT43U 報告 (外資及陸資買賣超彙總表)。
    URL: https://www.twse.com.tw/rwd/zh/fund/TWT43U
    
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/fund/TWT43U"
    url = f"{base_url}?date={target_date}&response=csv"

    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "9_TWT43U")
    filename = OUTPUT_DIR + f"\\{target_date}_TWT43U_ForeignTrade.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (9/10) TWT43U (外資買賣超) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # TWT43U 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=2)

    if df is not None and '證券代號' in df.columns:
        df = df[df['證券代號'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (9/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_twt44u(target_date: str) -> Optional[pd.DataFrame]:
    """
    (10/10) 抓取指定日期的 TWT44U 報告 (投信買賣超彙總表)。
    URL: https://www.twse.com.tw/rwd/zh/fund/TWT44U
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/fund/TWT44U"
    url = f"{base_url}?date={target_date}&response=csv"
    
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "10_TWT44U")
    filename = OUTPUT_DIR + f"\\{target_date}_TWT44U_InvestmentTrust.csv"

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 (10/10) TWT44U (投信買賣超) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    df = _read_twse_csv(response_text, header_row=1)

    if df is not None and '證券代號' in df.columns:
        df = df[df['證券代號'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (10/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_t86(target_date: str) -> Optional[pd.DataFrame]:
    """
    抓取指定日期的 T86 報告 (三大法人買賣超彙總表 - 依類別)。
    URL: https://www.twse.com.tw/rwd/zh/fund/T86
    
    :param target_date: 查詢日期，格式為 YYYYMMDD (例如: 20251031)
    :return: 包含三大法人買賣超資料的 DataFrame，如果失敗則為 None
    """
    
    if not re.fullmatch(r'\d{8}', target_date): 
        print("日期格式錯誤，請使用 YYYYMMDD 格式。")
        return None
        
    # 定義 URL 結構
    base_url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    url = f"{base_url}?date={target_date}&selectType=ALL&response=csv"
    
    # 定義檔案儲存路徑
    # 假設 "datas/raw/10_T86" 是相對於此腳本的位置
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "raw" , "11_T86")
    filename = os.path.join(OUTPUT_DIR, f"{target_date}_T86_InstitutionalTrades.csv")

    check_folder_and_create(filename)
    
    print(f"嘗試抓取 T86 (三大法人買賣超 - 依類別) 資料，日期: {target_date}...")
    
    # 1. 抓取資料
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # 2. 解析 CSV (header_row=1 表示欄位名稱在第二行)
    # T86 表格的欄位名稱通常在回傳的 CSV 內容的第二行
    df = _read_twse_csv(response_text, header_row=1)

    # 3. 數據清理與儲存
    if df is not None and '證券代號' in df.columns:
        # 清除沒有證券代號的空行
        df = df[df['證券代號'].astype(str).str.strip() != '']
        
        # 清理多餘的描述行（例如底部的合計行，其證券代號欄位可能為空）
        if '投信買賣超' in df.columns:
            # 確保數字欄位可以被轉換
            df['投信買賣超'] = pd.to_numeric(df['投信買賣超'], errors='coerce')
        
        # 刪除所有數字欄位皆為 NaN 的行 (可能是合計或無用訊息)
        df.dropna(subset=df.columns[2:], how='all', inplace=True)
        
        # 儲存 CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ {filename} 儲存成功。")
        return df
    
    print(f"❌ 數據處理失敗，可能該日期 ({target_date}) 為非交易日或網站資料結構改變。")
    return None

# 設定您想要抓取的目標日期 (只需修改此處即可抓取所有報告的資料)
# 1. 取得今天的日期並格式化為 YYYYMMDD

TARGET_DATE = date.today().strftime("%Y%m%d") 
#TARGET_STOCK = "2330" # 台灣積體電路製造
#TARGET_DATE = "20251023"  # 測試用特定日期

print("\n" + "="*50)
print("--- 程式開始執行：TWSE 10 大報告批量抓取 ---")
#print(f"🎯 目標查詢日期: {TARGET_DATE} | 股票代號: {TARGET_STOCK}")
print("="*50 + "\n")

# 設置一個列表來儲存結果，便於最終預覽
results = []

# 1. STOCK_DAY (個股日成交資訊)
# 改以單獨的程式抓取資料
#results.append(("STOCK_DAY", fetch_twse_stock_day(TARGET_DATE, TARGET_STOCK)))

file_path = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / "trading_day_2021-2025.csv"
# 假設您的日期欄位名稱就是 'Date'
#past_dates_yyyymmdd = get_past_dates_in_yyyymmdd(file_path, date_column_name='日期')
past_dates_yyyymmdd = TARGET_DATE
# past_dates_yyyymmdd = ["20251031",
#                        "20251030",
#                        "20251029",
#                        "20251028",
#                        "20251027",
#                        "20251023",
#                        "20251022",
#                        "20251021",
#                        "20251020",]

for every_day in past_dates_yyyymmdd:
    
    TARGET_DATE = every_day
    # 2. MI_INDEX (所有類股成交統計)
    results.append(("MI_INDEX", fetch_twse_mi_index(TARGET_DATE))) 

    # 3. BWIBBU_d (類股日成交量值及報酬率)
    results.append(("BWIBBU_d", fetch_twse_bwibbu_d(TARGET_DATE))) 

    # 4. MI_INDEX20 (收盤指數及成交量值資訊)
    results.append(("MI_INDEX20", fetch_twse_mi_index20(TARGET_DATE)))

    # 5. TWTASU (每日總成交量值與平均股價)
    results.append(("TWTASU", fetch_twse_twtasu(TARGET_DATE))) 

    # 6. BFIAMU (自營商買賣超彙總表)
    results.append(("BFIAMU", fetch_twse_bfiamu(TARGET_DATE))) 

    # 7. FMTQIK (每日券商成交量值總表)
    results.append(("FMTQIK", fetch_twse_fmtqik(TARGET_DATE)) )

    # 8. BFI82U (三大法人買賣超彙總表 - 日)
    results.append(("BFI82U", fetch_twse_bfi82u(TARGET_DATE)))

    # 9. TWT43U (外資及陸資買賣超彙總表)
    results.append(("TWT43U", fetch_twse_twt43u(TARGET_DATE)))

    # 10. TWT44U (投信買賣超彙總表)
    results.append(("TWT44U", fetch_twse_twt44u(TARGET_DATE)))

    # 11. T86 (三大法人買賣超彙總表)
    results.append(("T86", fetch_twse_t86(TARGET_DATE)))

    # 12. MI_MARGN (融資融券彙總 (全部))
    #results.append(("TWT93U", fetch_twse_mi_margn(TARGET_DATE)))


    # --- 最終結果預覽 ---
    print("\n" + "="*50)
    print("--- 10 個報告抓取結果摘要 ---")
    print("="*50)

    for name, df in results:
        if df is not None:
            print(f"\n[🟢 {name} (成功)] 數據筆數: {len(df)}")
            # print(df.head().to_markdown(index=False)) # 註釋掉避免輸出過多
        else:
            print(f"[🔴 {name} (失敗)] 無數據或抓取錯誤。")

    time.sleep(5) 

# 增加日誌儲存：記錄本次嘗試抓取的日期
log_summary_results(results, TARGET_DATE)

print("\n所有 CSV 檔案已儲存至程式執行目錄下。")
print("--- 程式執行結束 ---")
#==========================================================
# # 爬取並儲存上市/上櫃資料
# FILE_TYPES = ['exchange', 'counter']
# for stock_type in FILE_TYPES:
#     get_stocks_company_all.list_stock(stock_type)
# # 2. 合併所有儲存的檔案並進行篩選與日誌記錄
# get_stocks_company_all.combine_and_save(get_stocks_company_all.OUTPUT_csv_DIR, FILE_TYPES)
#==========================================================
