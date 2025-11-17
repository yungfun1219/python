import os
import pandas as pd
from typing import Union, Optional, List, Set, Dict, Tuple
import pathlib     # as pathlib
from datetime import datetime, timedelta

# --- 參數設定 ---

def _load_and_filter_single_day(
    file_path: str, 
    top_n: int,
    volume_column: str, 
    code_column: str
) -> Optional[pd.DataFrame]:
    """
    讀取、清理單一 CSV 檔案，並篩選出三大法人買超股數最大的 Top N 股票。

    Args:
        file_path (str): 單日三大法人買賣超數據檔案路徑。
        top_n (int): 要篩選出的前 N 名數量。
        volume_column (str): 買賣超股數欄位名稱。
        code_column (str): 證券代號欄位名稱。

    Returns:
        Optional[pd.DataFrame]: 包含 '代號', '名稱', '股數' 的 Top N DataFrame，若失敗則為 None。
    """
    try:
        # 讀取 CSV 檔案 (多編碼嘗試)
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='big5')
            
        required_cols = [volume_column, code_column, '證券名稱']
        if not all(col in df.columns for col in required_cols):
            return None

        # 數據清理與轉換
        df[volume_column] = (
            df[volume_column].astype(str).str.replace(r'[",\s]', '', regex=True)
        )
        df[volume_column] = pd.to_numeric(df[volume_column], errors='coerce')
        df[code_column] = df[code_column].astype(str).str.strip()
        
        df.dropna(subset=[volume_column], inplace=True)
        
        # 篩選條件：1. 代號為 4 位數字 | 2. 買賣超股數 > 0 (買超)
        df_filtered = df[
            (df[code_column].str.match(r'^\d{4}$')) & 
            (df[volume_column] > 1000)  # 假設 1000 股 (1 張) 為最小買超量，用於基礎篩選
        ].copy()

        # 排序並取出 Top N
        df_sorted = df_filtered.sort_values(
            by=volume_column, 
            ascending=False
        )
        
        top_n_data = df_sorted.head(top_n).rename(
            columns={code_column: '代號', '證券名稱': '名稱', volume_column: '股數'}
        )
        
        return top_n_data[['代號', '名稱', '股數']]
        
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的檔案 -> {os.path.basename(file_path)}")
        return None
    except Exception as e:
        print(f"❌ 數據處理失敗 ({os.path.basename(file_path)})：{e}")
        return None


def analyze_top_stocks_trend(
    file_paths: List[str],
    top_n: int = 30,  #往前20筆
    n_days_lookback: int = 5,  #往前追朔5天
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號"
) -> Optional[str]:
    """
    分析最新一日三大法人買超 Top N 股票在過去 N 天的回溯趨勢。

    Args:
        file_paths (List[str]): 依序為 [最新日, 前一日, ..., 前第 N 日] 的檔案路徑列表。
                                (至少需要 n_days_lookback + 1 個檔案路徑)
        top_n (int): 基準日要篩選出的前 N 名數量 (預設 30)。
        n_days_lookback (int): 要回溯的交易日天數 (預設 5)。
        volume_column (str): 買賣超股數欄位名稱。
        code_column (str): 證券代號欄位名稱。

    Returns:
        Optional[str]: 格式化輸出趨勢結果，或錯誤訊息。
    """
    
    # 確保有足夠的檔案進行基準日 + 回溯日分析
    required_files = n_days_lookback + 1
    if len(file_paths) < required_files:
        print(f"⚠️ 錯誤：至少需要 {required_files} 個檔案 (基準日 + {n_days_lookback} 個回溯日)。目前只有 {len(file_paths)} 個。")
        return None

    # --- 1. 處理所有交易日數據 ---
    all_day_data: Dict[int, Set[str]] = {}
    day_labels: List[str] = []
    
    print(f"🔍 開始處理 {required_files} 天數據...")
    
    for i, path in enumerate(file_paths[:required_files]):
        df_day = _load_and_filter_single_day(path, top_n, volume_column, code_column)
        
        # 提取檔案名稱中的日期作為標籤
        try:
             # 假設檔案名稱包含日期，例如 "20251110_institutional.csv"
            date_label = os.path.basename(path).split('_')[0][:8]
        except:
            date_label = f"Day-{i}"
            
        if df_day is None or df_day.empty:
            print(f"⚠️ {date_label} 數據載入或篩選失敗，將略過此日。")
            all_day_data[i] = set()
        else:
            # 將該日期的 Top N 股票代號存儲為集合 (Set)
            all_day_data[i] = set(df_day['代號'].tolist())
            print(f"✅ {date_label} 成功篩選出 {len(all_day_data[i])} 檔股票。")

        day_labels.append(date_label)
        
        # 基準日 (最新日) 的數據需要單獨保存，以便獲取 Top N 名稱和股數
        if i == 0:
            df_base_day = df_day
    
    if df_base_day is None or df_base_day.empty:
        print("❌ 基準日 (最新日) 數據無效或為空，無法進行趨勢分析。")
        return None
        
    # --- 2. 獲取基準日 Top N 股票代號 (此為分析的主體) ---
    base_stocks = df_base_day[['代號', '名稱', '股數']].head(top_n).copy()
    
    # --- 3. 執行回溯趨勢分析 ---
    
    # 建立一個新欄位用於存放趨勢標記 (從最新日往前 5 天)
    base_stocks['趨勢'] = "" 
    
    # 格式化輸出標題
    trend_header = ""
    
    for i in range(1, n_days_lookback + 1):
        # 回溯日期的標籤
        day_tag = day_labels[i].replace("/", "")[-4:] # 取日期後四碼 (如 1110)
        trend_header += f" | {day_tag} "
        
        # 取得該回溯日期的 Top N 股票代號集合
        past_top_n_codes = all_day_data.get(i, set())
        
        # 對基準日的每一檔股票進行檢查
        presence_markers = []
        for code in base_stocks['代號']:
            # 檢查代號是否出現在過去 Top N 列表中
            if code in past_top_n_codes:
                presence_markers.append("🔴") # 存在
            else:
                presence_markers.append("⚪️") # 不存在
        
        # 將標記串接起來
        base_stocks['趨勢'] += pd.Series(presence_markers, index=base_stocks.index)
    
    # --- 4. 格式化輸出結果 ---
    
    # 將股數轉換為張數並格式化
    volume_col_display_name = '買超張數'
    base_stocks[volume_col_display_name] = base_stocks['股數'].apply(lambda x: f"{int(x / 1000):,}")
    
    # 定義最終輸出欄位
    display_cols = ['代號', '名稱', volume_col_display_name, '趨勢']
    
    # 建立表格標頭
    output_lines = [
        f"\n📈 三大法人買超前{top_n}股(基準日:{day_labels[0]})-過去{n_days_lookback}日趨勢",
        "=" * 50,
        f"{'名次'.center(3)}|{'代號'.center(4)}|{'證券名稱'.center(8)}|{'買超張數'.center(8)}| 回溯趨勢 ({trend_header.strip()}) ",
        "-" * 50
    ]
    
    # 建立表格內容
    for rank, row in base_stocks.iterrows():
        rank_str = str(rank + 1).zfill(2)
        code_str = str(row['代號']).center(6)
        name_str = row['名稱'].ljust(8, '　') # 中文佔用寬度處理
        volume_str = row[volume_col_display_name].ljust(10)
        trend_str = row['趨勢'] # 🔴⚪️ 標記已包含間隔

        output_lines.append(f"{rank_str} |{code_str}| {name_str}|{volume_str} | {trend_str}")
        
    output_lines.append("=" * 50)
    output_lines.append(f"🔴: 該日出現在 Top {top_n} 名單中 | ⚪️: 該日未出現在 Top {top_n} 名單中")
    
    return "\n".join(output_lines)

# 根據指定日期與時間（21:00截止）提供往前6個交易日，前一個交易日則為df[-1]
def get_previous_n_trading_days(
    file_path: str,
    datetime_to_check: str,
    n_days: int = 6,        # 往前提出6個交易日
    CUTOFF_HOUR: int = 21,  # 設定截止時間為 21:00
    date_column_name: str = '日期') -> Union[List[str], None]:
    """
    根據指定日期與時間（21:00截止）確定一個有效查詢日期，
    並從該日期（含）開始向前追溯 N 個最近的交易日。
    Args:
        file_path (str): 包含交易日清單的 CSV 檔案完整路徑 (假設檔案中列出的是交易日)。
        datetime_to_check (str): 要檢查的日期和時間字串，例如 '2025/10/10 15:30:00'。
        n_days (int): 要獲取的上一個交易日的數量 (預設為 6)。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[List[str], None]: 包含 N 個交易日字串（'YYYY/MM/DD' 格式）的列表，
                                 或發生錯誤時回傳 None。
    """
    
    # 檢查檔案路徑
    if not os.path.exists(file_path):
        print(f"【錯誤】檔案路徑不存在，請確認路徑是否正確: {file_path}")
        return None
        
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。欄位有: {df.columns.tolist()}")
            return None
            
        # 確保日期欄位是字串，以避免格式不一致的問題
        df[date_column_name] = df[date_column_name].astype(str)

        # 設定日期格式
        input_dt_format = '%Y/%m/%d %H:%M:%S'
        input_date_format = '%Y/%m/%d'
        
        # 1. 解析輸入的日期時間
        try:
            input_dt = datetime.strptime(datetime_to_check, input_dt_format)
        except ValueError:
            print(f"【錯誤】輸入日期時間格式不正確。應為 '{input_dt_format}'。您輸入的是: {datetime_to_check}")
            return None
        
        input_date = input_dt.date()
        input_time = input_dt.time()
        
        # 2. 根據時間判斷「有效查詢日期」
        # 如果時間在 21:00 (含) 之後，有效日期為今天；否則為前一天。
        cutoff_time = input_dt.replace(hour=CUTOFF_HOUR, minute=0, second=0, microsecond=0).time()
        
        effective_check_date = input_date
        
        if input_time < cutoff_time:
            # 如果在 21:00 之前，視為前一天的交易
            effective_check_date = input_date - timedelta(days=1)
        
        # 3. 迴圈向前尋找最近的 N 個交易日
        current_check_date = effective_check_date
        trading_days_found: List[str] = []
        
        print(f"輸入日期時間: {datetime_to_check}")
        print(f"起始查詢日期 (根據 {CUTOFF_HOUR}:00 截止線判斷): {current_check_date.strftime(input_date_format)}")
        print(f"目標：向前追溯 {n_days} 個交易日...")

        max_lookback_days = n_days * 3  # 設定最大追溯天數，避免無限迴圈
        days_passed = 0

        while len(trading_days_found) < n_days:
            
            # 安全機制檢查
            if days_passed > max_lookback_days:
                print(f"【警告】已向前追溯超過 {max_lookback_days} 天 ({current_check_date.strftime(input_date_format)})，可能資料清單不完整。停止尋找。")
                break

            date_str = current_check_date.strftime(input_date_format)
            
            # 使用 isin 檢查日期是否存在於交易日清單中
            is_trading_day = df[date_column_name].isin([date_str]).any()
            
            if is_trading_day:
                # 找到交易日，添加到列表
                trading_days_found.append(date_str)
                print(f"✅ 找到第 {len(trading_days_found)} 個交易日: {date_str}")
            
            # 無論是否為交易日，都往前推一天，直到找到足夠的數量
            current_check_date -= timedelta(days=1)
            days_passed += 1

        # 將列表反轉，使其按時間順序排列 (如果需要的話，通常是從最早到最近)
        # 如果希望從最近到最舊，則不需要反轉
        #trading_days_found.reverse() 

        # 4. 判斷今天是否為交易日並回傳結果
        current_day_is_trading = df[date_column_name].isin([input_date.strftime(input_date_format)]).any()
        
        if current_day_is_trading:
             print(f"\n今天日期 ({input_date.strftime(input_date_format)}) 為交易日。")
        else:
             print(f"\n今天日期 ({input_date.strftime(input_date_format)}) 為休市日。")
        
        
        # 確保列表是從最舊到最新排列
        #trading_days_found.reverse()

        if len(trading_days_found) == n_days:
            # 完整收集到 N 天
            print(f"✅ 成功收集到 {n_days} 個交易日。")
            return trading_days_found
        else:
            # 未收集到 N 天 (通常是數據不足)
            print(f"⚠️ 僅找到 {len(trading_days_found)} 個交易日，數量不足 {n_days} 個。")
            return trading_days_found # 即使不足也回傳找到的結果

    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return None

HOLIDAYS_FILE = r'D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays\trading_day_2021-2025.csv'
Now_time_hour = datetime.now().strftime("%H")  #取得目前系統時間的「幾點鐘」
Now_day_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")  #取得目前系統時間的日期及時間「例如 2025-11-12 11:12」
    
print(f"--- 案例 4: 今天{datetime.now().strftime("%Y/%m/%d") }，現在時間{datetime.now().strftime("%H:%M:%S")} ---")
result = get_previous_n_trading_days(
    file_path=HOLIDAYS_FILE, 
    datetime_to_check=datetime.now().strftime("%Y/%m/%d %H:%M:%S")
)

T86_folder_base = pathlib.Path(__file__).resolve().parent / "datas" / "raw" / "11_T86"
mock_file_paths = []
for day_roll1 in result[0:-1]:
    day_roll1 = day_roll1.replace('/', '')
    file_path =  T86_folder_base / f"{day_roll1}_T86_InstitutionalTrades.csv"
    mock_file_paths.append(file_path)
    
analysis_result = analyze_top_stocks_trend(
    file_paths=mock_file_paths,
    top_n = 30, #定義要抓取的前20筆
    n_days_lookback= 4, #定義回溯天數
)

# 3. 輸出結果
if analysis_result:
    print(analysis_result)
