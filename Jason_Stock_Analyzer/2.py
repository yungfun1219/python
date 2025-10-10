import requests
import pandas as pd
from io import StringIO
import urllib3
import re
from datetime import date
from typing import Optional

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 配置 (Configuration) ---
LOG_FILENAME = "last_fetch_date.log" # 定義日誌檔案名稱

# --- 輔助函式 (Helper Functions) ---
def _read_twse_csv(response_text: str, header_row: int) -> Optional[pd.DataFrame]:
    """
    共用的輔助函式，用於處理 TWSE 的 Big5 編碼和 Pandas 讀取邏輯。
    
    Args:
        response_text: HTTP 請求回傳的文字內容 (Big5 編碼)。
        header_row: CSV 檔案中資料表頭所在的行數 (0-indexed)。

    Returns:
        Optional[pd.DataFrame]: 處理後的 DataFrame。
    """
    try:
        csv_data = StringIO(response_text)
        # 嘗試自動尋找標題列
        preview = response_text.splitlines()
        for i, line in enumerate(preview[:10]):
            if '證券代號' in line or '日期' in line or '類股名稱' in line:
                header_row = i
                break

        df = pd.read_csv(
            StringIO(response_text),
            header=header_row,
            encoding='Big5',
            skipinitialspace=True,
            on_bad_lines='skip'
        )
        df.columns = df.columns.str.strip()
        df = df.dropna(how='all')
        return df
    except Exception as e:
        print(f"讀取 CSV 錯誤: {e}")
        return None

def _fetch_twse_data(url: str) -> Optional[str]:
    """
    共用的輔助函式，用於發送 HTTP 請求並檢查狀態。
    
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

def log_fetch_date(fetch_date: str, filename: str = LOG_FILENAME):
    """將成功抓取資料的日期寫入日誌檔案。"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(fetch_date)
        print(f"\n[日誌] 成功將最後抓取日期 ({fetch_date}) 寫入檔案：{filename}")
    except Exception as e:
        print(f"❌ 寫入日誌檔案發生錯誤: {e}")

# --- 10 大 TWSE 報告抓取函式 (分頁與個股) ---

def fetch_twse_stock_day(target_date: str, stock_no: str) -> Optional[pd.DataFrame]:
    """
    (1/10) 抓取指定日期和股票代號的 STOCK_DAY 報告 (每日成交資訊)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
    url = f"{base_url}?date={target_date}&stockNo={stock_no}&response=csv"
    filename = f"{target_date}_{stock_no}_STOCK_DAY.csv"
    
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
    
    **修正:** 表頭索引改為 3 (原為 2)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
    url = f"{base_url}?date={target_date}&type=ALLBUT0999&response=csv"
    filename = f"{target_date}_MI_INDEX_Sector.csv"
    
    print(f"嘗試抓取 (2/10) MI_INDEX (類股統計) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # MI_INDEX 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=3)
    
    if df is not None and '類股名稱' in df.columns:
        df = df[df['類股名稱'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (2/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_bwibbu_d(target_date: str) -> Optional[pd.DataFrame]:
    """
    (3/10) 抓取指定日期的 BWIBBU_d 報告 (發行量加權股價指數類股日成交量值及報酬率)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d
    
    **修正:** 表頭索引改為 3 (原為 2)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
    url = f"{base_url}?date={target_date}&selectType=ALL&response=csv"
    filename = f"{target_date}_BWIBBU_d_IndexReturn.csv"
    
    print(f"嘗試抓取 (3/10) BWIBBU_d (類股報酬率) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # BWIBBU_d 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=1)
    if df is not None and '指數' in df.columns:
        df = df[df['指數'].astype(str).str.strip() != '']
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
    filename = f"{target_date}_MI_INDEX20_Market.csv"
    
    print(f"嘗試抓取 (4/10) MI_INDEX20 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None
    
    # MI_INDEX20 報表的表頭在索引 2
    df = _read_twse_csv(response_text, header_row=2) 
    if df is not None and '指數名稱' in df.columns:
        df = df[df['指數名稱'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (4/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_twtasu(target_date: str) -> Optional[pd.DataFrame]:
    """
    (5/10) 抓取指定日期的 TWTASU 報告 (每日總成交量值與平均股價)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU
    
    **修正:** 表頭索引改為 3 (原為 2)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU"
    url = f"{base_url}?date={target_date}&response=csv"
    filename = f"{target_date}_TWTASU_VolumePrice.csv"
    
    print(f"嘗試抓取 (5/10) TWTASU (總量值) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # TWTASU 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=3)
    if df is not None and '項目' in df.columns: 
        df = df[df['項目'].astype(str).str.strip() != '']
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
    filename = f"{target_date}_BFIAMU_DealerTrade.csv"
    
    print(f"嘗試抓取 (6/10) BFIAMU (自營商買賣超) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # BFIAMU 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=3)
    if df is not None and '自營商' in df.columns:
        df = df.dropna(how='all') 
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (6/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_fmtqik(target_date: str) -> Optional[pd.DataFrame]:
    """
    (7/10) 抓取指定日期的 FMTQIK 報告 (每日券商成交量值總表)。
    URL: https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK
    
    **修正:** 表頭索引改為 2 (原為 1)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
    url = f"{base_url}?date={target_date}&response=csv"
    filename = f"{target_date}_FMTQIK_BrokerVolume.csv"
    
    print(f"嘗試抓取 (7/10) FMTQIK (券商成交總表) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # FMTQIK 報表的表頭在索引 2
    df = _read_twse_csv(response_text, header_row=2)
    if df is not None and '券商代號' in df.columns:
        df = df[df['券商代號'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (7/10) {filename} 儲存成功。")
        return df
    return None

# --- 10 大 TWSE 報告抓取函式 (三大法人) ---

def fetch_twse_bfi82u(target_date: str) -> Optional[pd.DataFrame]:
    """
    (8/10) 抓取指定日期的 BFI82U 報告 (三大法人買賣超彙總表 - 日)。
    URL: https://www.twse.com.tw/rwd/zh/fund/BFI82U
    
    **修正:** 表頭索引改為 3 (原為 1)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U"
    # 只使用 dayDate 進行日期參數模組化
    url = f"{base_url}?type=day&dayDate={target_date}&response=csv"
    filename = f"{target_date}_BFI82U_3IParty_Day.csv"
    
    print(f"嘗試抓取 (8/10) BFI82U (三大法人日報) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # BFI82U 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=3)
    if df is not None and '證券代號' in df.columns:
        df = df[df['證券代號'].astype(str).str.strip() != '']
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ (8/10) {filename} 儲存成功。")
        return df
    return None

def fetch_twse_twt43u(target_date: str) -> Optional[pd.DataFrame]:
    """
    (9/10) 抓取指定日期的 TWT43U 報告 (外資及陸資買賣超彙總表)。
    URL: https://www.twse.com.tw/rwd/zh/fund/TWT43U
    
    **修正:** 表頭索引改為 3 (原為 1)。
    """
    if not re.fullmatch(r'\d{8}', target_date): return None
    base_url = "https://www.twse.com.tw/rwd/zh/fund/TWT43U"
    url = f"{base_url}?date={target_date}&response=csv"
    filename = f"{target_date}_TWT43U_ForeignTrade.csv"
    
    print(f"嘗試抓取 (9/10) TWT43U (外資買賣超) 資料...")
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # TWT43U 報表的表頭在索引 3
    df = _read_twse_csv(response_text, header_row=3)
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
    filename = f"{target_date}_TWT44U_InvestmentTrust.csv"
    
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

# --- 範例使用 (模組化參數化並呼叫所有 10 個函式) ---

# 設定您想要抓取的目標日期 (只需修改此處即可抓取所有報告的資料)
TARGET_DATE = "20251009" 
TARGET_STOCK = "2330" # 台灣積體電路製造

print("\n" + "="*50)
print("--- 程式開始執行：TWSE 10 大報告批量抓取 ---")
print(f"🎯 目標查詢日期: {TARGET_DATE} | 股票代號: {TARGET_STOCK}")
print("="*50 + "\n")

# 設置一個列表來儲存結果，便於最終預覽
results = []

# 1. STOCK_DAY (個股日成交資訊)
results.append(("STOCK_DAY", fetch_twse_stock_day(TARGET_DATE, TARGET_STOCK)))

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

# 增加日誌儲存：記錄本次嘗試抓取的日期
log_fetch_date(TARGET_DATE)

print("\n所有 CSV 檔案已儲存至程式執行目錄下。")
print("--- 程式執行結束 ---")
