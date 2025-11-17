import requests
import pandas as pd
import re
import time
from io import StringIO
from typing import Optional, Union, List
from datetime import date, datetime, time, timedelta
import warnings
from pathlib import Path

# 忽略 requests 啟用 verify=False 時可能出現的 SSL 警告
warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- 參數設定 ---
# 每次重試的延遲時間 (秒)
RETRY_DELAY_SECONDS = 3600 # 1 小時
# 最大重試次數
MAX_RETRIES = 5 
# 使用 Pathlib 定義基礎路徑 (確保與您提供的程式碼邏輯一致，使用 __file__)
BASE_DIR = Path(__file__).resolve().parent

# 定義交易日曆和數據儲存路徑
TRADING_DAY_FILE = BASE_DIR / "datas" / "processed" / "get_holidays" / "trading_day_2021-2025.csv" 
# 我們將嘗試獲取最近 5 個交易日的資料 (配合您的程式碼片段需求)
N_DAYS_TO_FETCH = 5 

# --- 輔助函式：路徑操作 ---

def check_folder_and_create(filepath: Path):
    """
    檢查路徑中的資料夾是否存在，不存在則創建。
    參數 filepath 必須為 pathlib.Path 物件。
    """
    output_dir = filepath.parent
    if not output_dir.exists():
        # parents=True 確保創建所有上層目錄，exist_ok=True 避免重複創建錯誤
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"創建資料夾: {output_dir}")

# --- 輔助函式：CSV 解析 ---

def _read_twse_csv(response_text: str, header_row: int = 1) -> Optional[pd.DataFrame]:
    """
    將 TWSE 回傳的 CSV 文字內容解析為 Pandas DataFrame。
    """
    try:
        csv_file = StringIO(response_text)
        # T86 表格的欄位名稱通常在回傳的 CSV 內容的第二行 (index 1)
        # skipfooter=1 是因為 TWSE CSV 最後一行通常是總計或其他無用資訊
        df = pd.read_csv(csv_file, header=header_row, skipfooter=1, engine='python')
        
        # 清除欄位名稱前後的空白
        df.columns = df.columns.str.strip()
        
        return df

    except Exception as e:
        print(f"❌ 解析 CSV 內容時發生錯誤: {e}")
        return None

def _read_local_csv(file_path: Path) -> Optional[pd.DataFrame]:
    """
    讀取本地 CSV 檔案，並處理不存在的情況。
    """
    if not file_path.exists():
        # print(f"❌ 錯誤：找不到檔案 {file_path}")
        return None
    try:
        # 假設本地儲存的 CSV 已經是 UTF-8-SIG 編碼
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        # 清除欄位名稱前後的空白
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        print(f"❌ 錯誤：讀取本地 CSV 檔案 {file_path} 時失敗: {e}")
        return None

# --- 輔助函式：網路請求 ---

def _fetch_twse_data(url: str) -> Optional[str]:
    """
    獲取 TWSE 資料，使用 Big5 編碼解碼。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # verify=False 是為了避免 SSL 驗證問題 (但應謹慎使用)
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status() 
        # TWSE 網站 CSV 內容通常是 Big5 編碼
        response.encoding = 'Big5'
        return response.text
    except requests.exceptions.HTTPError as errh:
        # response.status_code 404/403 通常表示當日無資料
        print(f"❌ HTTP 錯誤：{errh} (該日可能無交易資料)")
    except requests.exceptions.RequestException as err:
        print(f"❌ 連線或 Requests 錯誤: {err}")
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")

    return None

# --- 輔助函式：取得最近 N 個交易日 ---

def find_last_n_trading_days_with_time_check(file_path: Path, n: int = N_DAYS_TO_FETCH) -> Optional[pd.DataFrame]:
    """
    從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。
    :return: 包含最近 N 個交易日日期字串 (YYYY/MM/DD) 的 DataFrame，或 None。
    """
    now = datetime.now()
    today_date = now.date()
    cutoff_time = time(15, 0, 0)
    is_after_cutoff = now.time() >= cutoff_time

    print(f"\n--- 交易日判斷 ---")
    print(f"當前日期: {today_date.strftime('%Y/%m/%d')}, 當前時間是否在 15:00 之後: {is_after_cutoff}")
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except Exception as e:
        print(f"❌ 錯誤：無法讀取交易日檔案 {file_path}: {e}")
        return None

    date_column = '日期' 
    if date_column not in df.columns and 'Date' in df.columns:
        date_column = 'Date'
    elif date_column not in df.columns:
        print(f"❌ 錯誤：交易日檔案中找不到日期欄位。")
        return None
        
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.normalize()
    df.dropna(subset=[date_column], inplace=True)
    all_trading_dates = set(df[date_column].dt.date)
    is_today_trading_day = today_date in all_trading_dates
    
    print(f"今天 ({today_date.strftime('%Y/%m/%d')}) 是否為交易日: {is_today_trading_day}")

    inclusion_date = today_date - timedelta(days=1)
    
    if is_today_trading_day and is_after_cutoff:
        inclusion_date = today_date
        print("-> 判斷：納入今天的交易日。")
    else:
        inclusion_date = today_date - timedelta(days=1)
        print("-> 判斷：排除今天的交易日，只取昨天及更早的日期。")

    df_past = df[df[date_column].dt.date <= inclusion_date]
    df_past = df_past.sort_values(by=date_column, ascending=False)
    # 取最近 N 個交易日
    last_n_days = df_past.head(n).copy()

    if last_n_days.empty:
        print(f"⚠️ 警告：交易日資料不足，無法找到往前 {n} 個交易日。")
        return None
        
    # 確保回傳的 DataFrame 日期格式是 YYYY/MM/DD (與使用者片段中的 day_roll 邏輯一致)
    last_n_days['日期'] = last_n_days[date_column].dt.strftime('%Y/%m/%d')
    
    print(f"\n✅ 成功找到今天往前 {len(last_n_days)} 個交易日。")
    return last_n_days.rename(columns={date_column: '日期'})

# --- 核心抓取函式 (單次嘗試) ---

def fetch_twse_t86(target_date: str) -> Optional[pd.DataFrame]:
    """
    抓取指定日期的 T86 報告 (三大法人買賣超彙總表 - 依類別)，並儲存為 CSV。
    """
    
    if not re.fullmatch(r'\d{8}', target_date): 
        print("日期格式錯誤，請使用 YYYYMMDD 格式。")
        return None
        
    # 定義 URL 結構
    base_url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    url = f"{base_url}?date={target_date}&selectType=ALL&response=csv"
    
    # 輸出路徑
    OUTPUT_DIR = BASE_DIR / "datas" / "raw" / "11_T86"
    filename: Path = OUTPUT_DIR / f"{target_date}_T86_InstitutionalTrades.csv"

    check_folder_and_create(filename)
    
    print(f"-> 嘗試從 TWSE 獲取資料...")
    
    # 1. 抓取資料
    response_text = _fetch_twse_data(url)
    if response_text is None: return None

    # 2. 解析 CSV
    df = _read_twse_csv(response_text, header_row=1)

    # 3. 數據清理與儲存
    if df is not None and '證券代號' in df.columns:
        # 清除沒有證券代號的空行
        df = df[df['證券代號'].astype(str).str.strip() != '']
        
        # 確保數字欄位可以被轉換，清除總計行等
        numeric_cols = [col for col in df.columns if '買賣超' in col or '金額' in col]
        if numeric_cols:
            for col in numeric_cols:
                 # 清除逗號後轉換為數字，不能轉換的設為 NaN
                df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
            # 刪除所有數字欄位皆為 NaN 的行 (可能是合計或無用訊息)
            df.dropna(subset=numeric_cols, how='all', inplace=True)
            
            # 如果數據清理後仍有資料
            if not df.empty:
                # 儲存 CSV
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"✅ {filename} 儲存成功，共 {len(df)} 筆資料。")
                return df
    
    print(f"❌ 數據處理失敗，可能該日期 ({target_date}) 為非交易日或網站資料結構改變。")
    return None

# --- 重試機制主函數 ---

def fetch_t86_with_retry(target_date: str) -> Optional[pd.DataFrame]:
    """
    嘗試抓取 T86 資料，失敗時等待 1 小時後重試。
    """
    retries = 0
    while retries < MAX_RETRIES:
        print(f"\n--- 嘗試抓取 T86 數據 (日期: {target_date}, 第 {retries + 1} 次嘗試) ---")
        
        df = fetch_twse_t86(target_date)
        
        # 判斷是否成功
        if df is not None and not df.empty:
            return df
        
        retries += 1
        
        if retries < MAX_RETRIES:
            delay_hours = RETRY_DELAY_SECONDS // 3600
            print(f"⚠️ 抓取失敗，等待 {delay_hours} 小時後重試 (下次重試時間: {datetime.now() + timedelta(seconds=RETRY_DELAY_SECONDS)})。")
            time.sleep(RETRY_DELAY_SECONDS)
        else:
            print(f"❌ 已達最大重試次數 ({MAX_RETRIES})，放棄抓取日期 {target_date} 的資料。")
            return None
    return None

# --- 新增的本地數據處理函式 (配合使用者片段) ---

def lookup_stock_price(file_path: Path, stock_name: str, name_col: str, price_col: str) -> Optional[str]:
    """
    從指定的 BWIBBU CSV 檔案中查找特定股票的收盤價。
    """
    df = _read_local_csv(file_path)
    if df is None:
        print(f"  > 警告: 價格檔案 {file_path.name} 缺失或讀取失敗。")
        return None

    try:
        # 尋找匹配的股票名稱
        result = df[df[name_col] == stock_name]
        if not result.empty and price_col in result.columns:
            # 返回收盤價，並格式化為字串
            price = result.iloc[0][price_col]
            # 確保價格是數字格式
            if pd.isna(price) or not pd.api.types.is_numeric_dtype(result[price_col].dtype):
                return str(float(price)) if price is not None else None
            return f"{price:.2f}"
        else:
            # print(f"  > 警告: 找不到 {stock_name} 的價格資料。")
            return None
    except Exception as e:
        print(f"  > 價格查詢失敗: {e}")
        return None

def get_stock_net_volume(file_path: Path, stock_name: str) -> Optional[pd.Series]:
    """
    從 T86 CSV 檔案中獲取特定股票的三大法人買賣超彙總股數。
    這裡假設 T86 檔案中的「三大法人買賣超」是所有法人彙總的股數。
    """
    df = _read_local_csv(file_path)
    if df is None:
        # print(f"  > 警告: 買賣超檔案 {file_path.name} 缺失或讀取失敗。")
        return None

    try:
        # T86 的欄位應為「證券名稱」和「三大法人買賣超股數」
        name_col = '證券名稱'
        volume_col = '三大法人買賣超股數'

        if name_col not in df.columns or volume_col not in df.columns:
             print(f"  > 警告: T86 檔案 {file_path.name} 欄位不完整。")
             return None

        result = df[df[name_col] == stock_name]
        
        if not result.empty:
            # 返回買賣超股數 Series
            return result[volume_col].iloc[0] # 返回單一數值

    except Exception as e:
        print(f"  > 買賣超查詢失敗: {e}")
        return None
    
    return None # 找不到資料

def get_stock_indicators(file_path: Path, stock_name: str) -> Optional[pd.DataFrame]:
    """
    從指定的 BWIBBU CSV 檔案中查找特定股票的股價指標（殖利率、本益比、股價淨值比）。
    """
    df = _read_local_csv(file_path)
    if df is None:
        # print(f"  > 警告: 指標檔案 {file_path.name} 缺失或讀取失敗。")
        return None

    try:
        # 假設 BWIBBU 檔案中的欄位
        name_col = '證券名稱' 
        indicator_cols = ['殖利率(%)', '本益比', '股價淨值比']

        if not all(col in df.columns for col in indicator_cols):
             print(f"  > 警告: 指標檔案 {file_path.name} 缺少必要的指標欄位。")
             return None

        result = df[df[name_col] == stock_name]
        
        if not result.empty:
            # 返回相關指標的 DataFrame
            return result[indicator_cols].head(1)
            
    except Exception as e:
        print(f"  > 指標查詢失敗: {e}")
        return None
    
    return None # 找不到資料

def get_top_20_institutional_trades_filtered(file_path: Path) -> Optional[pd.DataFrame]:
    """
    從 T86 CSV 檔案中獲取三大法人淨買超（買超減賣超 > 0）前 20 名的資料。
    """
    df = _read_local_csv(file_path)
    if df is None:
        # print(f"  > 警告: T86 檔案 {file_path.name} 缺失或讀取失敗。")
        return None

    try:
        # 假設 T86 檔案中已有「三大法人買賣超股數」欄位
        volume_col = '三大法人買賣超股數'
        name_col = '證券名稱'
        
        if volume_col not in df.columns:
            print(f"  > 警告: T86 檔案 {file_path.name} 缺少 {volume_col} 欄位。")
            return None

        # 過濾出淨買超 (數值大於 0)
        df_positive = df[pd.to_numeric(df[volume_col], errors='coerce') > 0].copy()
        
        # 轉換為數值，並以股數排序 (降冪)
        df_positive['Volume'] = pd.to_numeric(df_positive[volume_col], errors='coerce')
        df_positive.sort_values(by='Volume', ascending=False, inplace=True)
        
        # 格式化輸出
        top_20 = df_positive.head(20).reset_index(drop=True)
        
        # 只保留證券代號、名稱和買賣超股數（換算成張）
        if '證券代號' in top_20.columns:
            output_cols = ['證券代號', name_col, volume_col]
        else:
            output_cols = [name_col, volume_col]
            
        top_20 = top_20[output_cols]
        
        # 將股數除以 1000 轉換為張數
        top_20['淨買超(張)'] = (pd.to_numeric(top_20[volume_col], errors='coerce') / 1000).round(0).astype(int)
        
        # 選擇最終輸出欄位
        final_cols = [col for col in ['證券代號', name_col, '淨買超(張)'] if col in top_20.columns]
        
        return top_20[final_cols].rename(columns={name_col: '證券名稱'}).to_string(index=False)

    except Exception as e:
        print(f"  > 取得買超前 20 名失敗: {e}")
        return None
    
    return None

# --- 範例執行區 ---
if __name__ == "__main__":
    
    # 範例變數 (供測試用)
    TARGET_STOCK_NAMES = ["台積電", "鴻海", "聯發科"]
    Now_day_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    print(f"==================================================")
    print(f"啟動 TWSE T86 資料抓取與分析程式")
    print(f"設定: 查詢最近 {N_DAYS_TO_FETCH} 個交易日, 關注股票: {TARGET_STOCK_NAMES}")
    print(f"==================================================")

    # 1. 取得最近的交易日清單 (DataFrame 格式)
    # 這裡只進行日期判斷，但為了讓後續邏輯能運行，我們必須假設 T86 資料已經被抓下來了
    recent_trading_days_df = find_last_n_trading_days_with_time_check(TRADING_DAY_FILE, n=N_DAYS_TO_FETCH)
    
    if recent_trading_days_df is None or recent_trading_days_df.empty:
        print("❌ 無法取得有效的交易日清單，程序終止。請檢查交易日曆檔案。")
    else:
        # 由於您的程式碼需要 T86 資料，這裡先呼叫抓取邏輯確保 T86 檔案存在
        date_list_for_fetch = recent_trading_days_df['日期'].apply(lambda x: x.replace("/", "")).tolist()
        print(f"\n--- 檢查並抓取 T86 數據 (日期: {date_list_for_fetch}) ---")
        
        # 註釋掉實際的 time.sleep 以加快測試，如果需要實際延遲，請取消註釋
        for d in date_list_for_fetch:
             # fetch_t86_with_retry(d) # 實際執行時需開啟，以確保檔案存在

             # 為了運行分析邏輯，我們假設 T86 檔案已經在 datas/raw/11_T86/ 存在
             t86_path = BASE_DIR / "datas" / "raw" / "11_T86" / f"{d}_T86_InstitutionalTrades.csv"
             if not t86_path.exists():
                 print(f"  > 警告: T86 檔案 {t86_path.name} 不存在，分析將跳過此日期。")
        
        
        # --- 2. 執行使用者提供的數據處理與報告生成迴圈 ---
        Send_message_ALL = ""
        for TARGET_STOCK_NAME in TARGET_STOCK_NAMES:
            Send_message = ""
            
            # -- 取得五個交易日的 YYYYMMDD 格式列表 --
            day_roll = []
            for date_str in recent_trading_days_df["日期"]:
                # 將 YYYY/MM/DD 轉為 YYYYMMDD
                day_roll.append(date_str.replace("/", "")) 

            if recent_trading_days_df is not None:
                print(f"\n--{TARGET_STOCK_NAME}最近{N_DAYS_TO_FETCH}個交易日--")

            # 取得最舊交易日 (day_roll[0]) 的收盤價作為比較基期
            CSV_NAME_COLUMN = "證券名稱"
            CSV_PRICE_COLUMN = "收盤價"
            
            CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll[0]}_BWIBBU_d_IndexReturn.csv"
            get_price_before = lookup_stock_price(
                file_path=CSV_PATH,
                stock_name=TARGET_STOCK_NAME,
                name_col=CSV_NAME_COLUMN,
                price_col=CSV_PRICE_COLUMN
            )

            if get_price_before is None:
                print(f"❌ 警告: 找不到基期價格 ({day_roll[0]})，跳過 {TARGET_STOCK_NAME} 分析。")
                continue # 跳到下一支股票
                
            print("基期收盤價:", get_price_before)
            
            total_price_percent = 0
            
            # 從第二個日期 (day_roll[1]) 開始迭代，計算每日漲跌幅
            for day_roll1 in day_roll[1:]:
                # 股價/指標檔案路徑 (BWIBBU)
                CSV_PATH_BWIBBU = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll1}_BWIBBU_d_IndexReturn.csv"
                # 法人買賣超檔案路徑 (T86)
                file_path_T86 = BASE_DIR / "datas" / "raw" / "11_T86" / f"{day_roll1}_T86_InstitutionalTrades.csv"
                stock_name = TARGET_STOCK_NAME # 目標證券名稱
                net_volume_data_value = 0 # 初始化淨買賣超股數

                # --- 讀取買賣超資料 (股數) ---
                net_volume_raw = get_stock_net_volume(file_path_T86, stock_name)

                if net_volume_raw is not None:
                    try:
                        # 1. 轉換為數值 (float)，並除以 1000 換算成「張」
                        net_volume_in_lots = float(net_volume_raw) / 1000
                        
                        # 2. 四捨五入取整數
                        rounded_lots = round(net_volume_in_lots)
                        
                        # 3. 格式化為帶「張」的字串
                        net_volume_data = f"{rounded_lots}張"
                        net_volume_data_value = rounded_lots # 儲存數值用於後續處理（如果需要）

                    except (ValueError, TypeError):
                        net_volume_data = "資料錯誤"
                        print(f"❌ 錯誤：T86 數據中包含無法轉換為數值的資料。")
                else:
                    net_volume_data = "無資料"
                    # print(f"找不到 {stock_name} 的買賣超股數資料或資料為空。")
                
                # --- 取得當日收盤價 ---
                get_price = lookup_stock_price(
                    file_path=CSV_PATH_BWIBBU,
                    stock_name=TARGET_STOCK_NAME,
                    name_col=CSV_NAME_COLUMN,
                    price_col=CSV_PRICE_COLUMN
                )

                if get_price is None:
                    print(f"⚠️ 警告: 找不到 {stock_name} 於 {day_roll1} 的收盤價，跳過該日計算。")
                    continue

                day_mmdd = f"{day_roll1[4:6]}/{day_roll1[-2:]}"
                
                try:
                    price_percent = (float(get_price) - float(get_price_before)) / float(get_price_before) * 100
                    price_percent = round(float(price_percent), 1)
                    
                    total_price_percent += price_percent
                    
                    if price_percent > 0:
                        price_percent_str = f"🔴+{abs(price_percent)}" # 正數帶 + 號
                    else:
                        price_percent_str = f"🟢{price_percent}" # 負數自帶 - 號
                        
                    Send_message += f"{day_mmdd}: {get_price} ({price_percent_str}%) ({net_volume_data})\n"
                    get_price_before = get_price # 更新基期價格
                    
                except (ValueError, TypeError, ZeroDivisionError) as e:
                    print(f"❌ 警告: 價格計算失敗 (價格: {get_price}, 基期價格: {get_price_before})，錯誤: {e}")
                    Send_message += f"{day_mmdd}: {get_price} (計算失敗)\n"
                    continue
                
            # --- 取得個股指標 ---
            # 使用最新的交易日 (day_roll[-1]) 的 BWIBBU 檔案
            latest_bwibbu_path = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll[-1]}_BWIBBU_d_IndexReturn.csv"
            stock_indicators_df = get_stock_indicators(latest_bwibbu_path, stock_name)
            
            if stock_indicators_df is not None and not stock_indicators_df.empty:
                pa_ratio = stock_indicators_df.iloc[0]['殖利率(%)']
                pe_ratio = stock_indicators_df.iloc[0]['本益比']
                pb_ratio = stock_indicators_df.iloc[0]['股價淨值比']
            else:
                pa_ratio, pe_ratio, pb_ratio = "N/A", "N/A", "N/A"
            
            message_add = f"\n--🎯【{stock_name}】個股資訊 🎯--\n" \
                          f"  本益比  : {pe_ratio}\n" \
                          f"股價淨值比: {pb_ratio}\n" \
                          f"  殖利率  : {pa_ratio}\n\n"
            
            # --- 總績效計算與格式化 ---
            if total_price_percent > 0:
                total_price_percent_str = f"🔴 +{round(total_price_percent, 1)}%"
            else:
                total_price_percent_str = f"🟢 {round(total_price_percent, 1)}%"
                
            # 呼叫函式 (使用最新的 T86 檔案)
            top_20_positive_df_str = get_top_20_institutional_trades_filtered(file_path_T86)
            
            # --- 彙總訊息 ---
            Send_message_ALL += f"發送時間: {Now_day_time}\n"
            Send_message_ALL += f"***************************\n"
            Send_message_ALL += f"📦 {day_roll[-1]} (庫存股)通知📦\n" # 使用最新的日期作為通知日期
            Send_message_ALL += f"***************************\n"
            Send_message_ALL += f"\n=🥇【{TARGET_STOCK_NAME}】最近{len(day_roll)-1}日收盤價🥇 =\n{Send_message}"
            Send_message_ALL += f"== 近{len(day_roll)-1}日總績效: {total_price_percent_str} ==\n"
            Send_message_ALL += message_add  # 加入個股資訊
            
        # 針對關注的股票，取得近5日收盤價
        Send_message_ALL += f"*****************************\n"
        Send_message_ALL += f"💡💡 {day_roll[-1]} 關注股資訊 (法人買超前 20 名)💡💡\n"
        Send_message_ALL += f"*****************************\n"
        Send_message_ALL += f"{top_20_positive_df_str}\n" # 加入前 20 名清單
        
        print("\n==================== 最終報告 ====================")
        print(Send_message_ALL)
        print("==================================================")