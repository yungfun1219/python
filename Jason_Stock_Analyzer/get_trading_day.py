import pathlib
import pandas as pd
import requests
import os
from io import StringIO
from typing import Optional
import datetime
from datetime import date, timedelta
import re
import sys #--測試程式中斷用sys.exit(0) #中斷程式
import urllib3

# 禁用 urllib3 的 InsecureRequestWarning 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 函式整理 --- 

# 從台灣證券交易所網站抓取指定年份的休市日期資料。
def fetch_twse_holidays(year: int) -> Optional[pd.DataFrame]:
    """
    從台灣證券交易所網站抓取指定年份的休市日期資料。
    TWSE 的 CSV 格式通常在開頭有幾行描述，需要特別處理。
    Args:
        year (int): 要抓取的年份 (例如 2024)。
    Returns:
        Optional[pd.DataFrame]: 包含休市日期的 DataFrame，如果抓取失敗則回傳 None。
    """
    # TWSE 網址結構：我們使用該年份的第一天作為查詢基準點
    twse_url = (
        f"https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
        f"?date={year}0101&response=csv"
    )
    
    print(f"📡 嘗試抓取 {year} 年 TWSE 休市資料...")
 
    try:
        # 使用 requests 獲取內容
        # *** 解決 SSLCertVerificationError 錯誤：加入 verify=False ***
        response = requests.get(twse_url, verify=False)
        
        # 檢查 HTTP 狀態碼
        if response.status_code != 200:
            print(f"❌ 網路請求失敗，狀態碼: {response.status_code}")
            return None
        
        # 將內容從 cp950 (Big5 擴充) 解碼為字串，以便 Pandas 讀取
        csv_data_string = response.content.decode('cp950', errors='ignore')
        
        # 使用 StringIO 模擬檔案，讓 Pandas 讀取字串
        data_io = StringIO(csv_data_string)

        # 讀取 CSV。TWSE CSV 通常第一行是描述，第二行才是真正的欄位名稱 (Header)
        df = pd.read_csv(data_io, header=1, encoding='cp950')
        
        # 數據清理步驟：
        
        # 1. 移除最後幾行可能出現的空白或註釋行 (通常會以 NaN 呈現)
        df.dropna(how='all', inplace=True)
        
        # 2. 重新命名欄位為中文 (假設前四個欄位分別是日期、名稱、說明、備註)
        if len(df.columns) >= 4:
            df.columns = ['日期', '名稱', '說明', '備註']
        
        # 3. 確保日期欄位為標準格式 (如果需要)
        # df['日期'] = pd.to_datetime(df['日期'], errors='coerce')

        print(f"✅ 成功抓取 {len(df)} 筆休市資料。")
        return df

    except Exception as e:
        print(f"❌ 抓取或處理資料時發生錯誤: {e}")
        return None

# 檢查指定資料夾內所有檔案名稱是否包含特定的關鍵字。
def check_files_for_keyword(folder_path: pathlib.Path, keyword: str):
    """
    檢查指定資料夾內所有檔案名稱是否包含特定的關鍵字。
    Args:
        folder_path: 目標資料夾的 Path 物件。
        keyword: 要尋找的字串。
    Returns:
        bool: 如果有任何檔案名稱包含關鍵字則回傳 True，否則回傳 False。
    """
    #print(f"--- 開始檢查資料夾: {folder_path} ---")
    # 檢查資料夾是否存在
    if not folder_path.is_dir():
        print(f"錯誤：指定的路徑不是一個有效的資料夾或不存在：{folder_path}")
        return

    found_count = 0
    
    # 使用 iterdir() 迭代資料夾內容
    for item in folder_path.iterdir():
        if item.is_file():
            file_name = item.name  # 取得檔案名稱 (Pathlib 的屬性)
            
            # 判斷檔案名稱是否包含關鍵字
            if keyword in file_name:
                #print(f"[✅ 包含] 檔案名稱：{file_name}")
                found_count += 1
            #else:
                #print(f"[❌ 不含] 檔案名稱：{file_name}")

    if found_count > 0:
        return False
    else:
        return True

# 將 DataFrame 儲存為本地 CSV 檔案的函式
def save_dataframe_to_csv(df: pd.DataFrame, year: int, filename: str = None):
    """
    將 DataFrame 儲存為本地 CSV 檔案。
    *** 修正編碼為 cp950 以解決本地開啟亂碼問題 ***
    """
    if filename is None:
        filename = pathlib.Path(os.path.dirname(os.path.abspath(__file__)), "datas", "twse_holidays" , f"twse_holidays_{year}.csv")
    
    try:
        # 使用 cp950 儲存，以確保在傳統中文作業系統上開啟時不會亂碼
        df.to_csv(filename, index=False, encoding='cp950')
        print(f"💾 資料已成功儲存至 {filename}")
    except Exception as e:
        print(f"❌ 儲存檔案失敗: {e}")

#     處理包含 '月日' 格式日期的 DataFrame，將其轉換為西元年份格式，並返回新的 DataFrame。
def transform_twse_holiday_dates(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    處理包含 '月日' 格式日期的 DataFrame，將其轉換為西元年份格式，並返回新的 DataFrame。

    參數:
        df (pd.DataFrame): 包含 '日期' 欄位的 DataFrame (例如: '1月1日')。
        year (int): 用於日期轉換的西元年份。

    返回:
        pd.DataFrame: 包含 '原始日期'、'名稱' 和 '西元日期' 欄位的新 DataFrame。
                      西元日期格式為 'YYYY/MM/DD'。
    """
    
    # 檢查 DataFrame 是否有效
    if df is None or df.empty:
        print(f"⚠️ 警告: 輸入的 DataFrame 為空或 None，返回一個空的 DataFrame。")
        # 為了保持函式返回型別一致，返回一個包含預期欄位的空 DataFrame
        return pd.DataFrame(columns=['原始日期', '名稱', '西元日期'])
    
    print(f"debug: 開始處理 {year} 年的資料...")
    
    # 使用 .copy() 避免修改傳入的原始 DataFrame
    df_transformed = df.copy()
    
    dates_md = df_transformed['日期']
    start_year_str = str(year)
    
    # 定義正則表達式，用於提取月份和日期 (例如：10月10日)
    # (\d+)月(\d+)日
    pattern = r"(\d+)月(\d+)日"
    
    dates_gregorian_list = []
    
    # 遍歷日期欄位，進行轉換
    for index, date_string in dates_md.items():
        print(f"debug: 索引 {index}，原始字串: {date_string}")

        match = re.search(pattern, date_string)
        if match:
            # 擷取月份和日期，並使用 zfill(2) 補零，確保格式為 MM 和 DD
            month_str = match.group(1).zfill(2)
            day_str = match.group(2).zfill(2)
            
            # 轉換為西元日期格式 YYYY/MM/DD
            dates_gregorian = f"{start_year_str}/{month_str}/{day_str}"
            print(f"  轉換為西元日期: {dates_gregorian}")
        else:
            dates_gregorian = None # 如果格式不符，則設定為 None
            print("❌ 錯誤：字串格式與預期不符，未找到月份和日期數字。")
        
        dates_gregorian_list.append(dates_gregorian)

    # 將轉換後的日期新增至 DataFrame 的新欄位
    df_transformed['日期'] = dates_gregorian_list
    
    # 重新命名原始欄位，使輸出的 DataFrame 結構更清晰
    df_transformed.rename(columns={'日期': '日期', '名稱': '假日名稱'}, inplace=True)
    
    print("\n✅ 日期轉換完成。")
    return df_transformed

# 新增：儲存日期清單到 CSV 檔案的函式
def save_dates_to_csv(dates_list: list[str], year: int, filename: str = 'weekend_dates.csv'):
    """
    將日期清單儲存為 CSV 檔案。
    Args:
        dates_list (list[str]): 要儲存的日期清單。
        year (int): 生成日期的年份 (用於檔案命名或日誌)。
        filename (str): 儲存的檔案名稱 (預設為 'weekend_dates.csv')。
    """
    if not dates_list:
        print("【警告】日期清單為空，不執行儲存操作。")
        return

    try:
        # 建立 DataFrame
        # 這裡假設您希望 CSV 檔案只有一欄，欄位名稱為 'Date'
        df = pd.DataFrame({
            '日期': dates_list,
            '假日名稱': ['週末'] * len(dates_list)
            })
        
        # 定義檔案路徑，儲存在當前目錄
        file_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__)), "datas", "processed" , "get_holidays" , f"{year}_{filename}")
        # 儲存為 CSV 檔案
        # index=False 表示不寫入行索引
        # encoding='utf-8-sig' 可確保中文或特殊字元在 Excel 中正確顯示
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"\n💾 成功將 {len(dates_list)} 筆週末日期儲存到 CSV 檔案。")
        print(f"檔案路徑: {file_path}")
        
    except Exception as e:
        print(f"【錯誤】儲存 CSV 檔案時發生錯誤: {e}")

# --- 函式：生成全年度週末日期清單 ---
def generate_full_year_weekend_dates(year: int) -> list[str]:
    """
    生成指定年份的所有週末日期清單 (星期六和星期日)。
    Args:
        year (int): 要生成日期的年份 (例如 2024)。
    Returns:
        list[str]: 包含所有週末日期的列表，格式為 'YYYY/MM/DD'。
    """
    print(f"\n📅 正在生成 {year} 年的全年度週末日期清單...")
    try:
        # 設置起始日和結束日
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # 使用 Pandas date_range 產生所有日期，頻率設為 'D' (Day)
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 過濾日期：保留星期六 (5) 和星期日 (6)。 (星期一=0, 星期日=6)
        all_dates = all_dates[all_dates.dayofweek.isin([5, 6])]

        # 將日期格式化為 'YYYY/MM/DD' 字串列表
        dates_list = all_dates.strftime('%Y/%m/%d').tolist()
        print(f"✅ 成功生成 {len(dates_list)} 筆日期 (僅包含週末)。")
        return dates_list
    except Exception as e:
        print(f"❌ 生成日期清單時發生錯誤: {e}")
        return []

#  讀取 CSV 檔案，從第二行 (即數據行) 開始提取指定的欄位資料。
def extract_columns_from_second_row(file_path: str, columns_to_extract: list = [0, 1]) -> pd.DataFrame:
    """
    讀取 CSV 檔案，從第二行 (即數據行) 開始提取指定的欄位資料。
    Args:
        file_path (str): CSV 檔案的完整路徑。
        columns_to_extract (list): 要提取的欄位索引清單 (從 0 開始計算)。
                                  預設為 [0, 1]，即第 1 欄和第 2 欄。
    Returns:
        pd.DataFrame: 包含提取出的第 1 和第 2 欄資料的 DataFrame。
    """
    try:
        # 嘗試使用 UTF-8-sig 讀取，若失敗則嘗試 cp950
        try:
            df = pd.read_csv(
            file_path,
            header=None,         # 將所有行都視為數據行 (包括原始的第一行)
            usecols=columns_to_extract, # 僅讀取指定的欄位
            encoding='UTF-8' # 確保正確解碼中文
        )
            
        except UnicodeDecodeError:
            df = pd.read_csv(
            file_path,
            header=None,         # 將所有行都視為數據行 (包括原始的第一行)
            usecols=columns_to_extract, # 僅讀取指定的欄位
            encoding='cp950' # 確保正確解碼中文
        )

        # 2. 由於 'header=None' 會將原始檔案的第一行 (通常是表頭) 視為數據，
        #    要從第二行開始，實際上就是移除 DataFrame 的第 0 行。
        #    我們使用 .iloc[1:] 來取得從索引 1 開始的所有行。
        data_df = df.iloc[1:]
        
        # 3. 重新命名欄位 (可選，但讓輸出更清晰)
        # 這裡假設原始 CSV 的第一行有表頭，我們取其值來當作新的欄位名稱
        # 由於我們已經讀入了所有資料，原始的欄位名稱位於 df.iloc[0]
        column_names = df.iloc[0].tolist() 
        data_df.columns = column_names
        
        # 4. 重設索引，使其從 0 開始連續
        data_df = data_df.reset_index(drop=True)
        
        print(f"✅ 成功提取檔案 {os.path.basename(file_path)} 的第 1 和第 2 欄資料" ,f"總共提取 {len(data_df)} 筆數據行 (從第二行開始)。")
        
        return data_df

    except FileNotFoundError:
        print(f"【錯誤】檔案未找到: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return pd.DataFrame()

# 讀取 holidays_all.csv 檔案，移除 '日期' 欄位的重複項，並覆寫原檔案。
def remove_duplicate_dates():
    """
    讀取 holidays_all.csv 檔案，移除 '日期' 欄位的重複項，並覆寫原檔案。
    """
    print(f"\n--- 正在清理重複日期: {CLEAN_FILE_NAME} ---")

    if not FILE_PATH.exists():
        print(f"【錯誤】找不到檔案: {FILE_PATH.relative_to(BASE_DIR_get_holidays)}")
        print("請確認檔案路徑和名稱是否正確。")
        return

    try:
        # 嘗試使用 UTF-8 (帶 BOM) 或 Big5 讀取，以處理可能的編碼問題
        try:
            df = pd.read_csv(FILE_PATH, encoding='utf-8-sig')
        except UnicodeDecodeError:
            df = pd.read_csv(FILE_PATH, encoding='big5')

        # 清理欄位名稱，移除可能存在的隱藏空格
        df.columns = df.columns.str.strip()

        if '日期' not in df.columns:
            print(f"【錯誤】檔案中找不到 '日期' 欄位。檔案欄位為: {df.columns.tolist()}")
            return
        
        original_count = len(df)
        
        # 執行重複項移除，僅保留第一次出現的記錄
        df_cleaned = df.drop_duplicates(subset=['日期'], keep='first')
        
        cleaned_count = len(df_cleaned)
        removed_count = original_count - cleaned_count

        if removed_count > 0:
            # 儲存清理後的檔案
            # 覆寫原檔案，使用 UTF-8 附帶 BOM
            df_cleaned.to_csv(FILE_PATH, index=False, encoding='utf-8-sig')
            
            print(f"✅ 清理完成！")
            print(f" 原始筆數: {original_count}")
            print(f" 移除重複筆數: {removed_count}")
            print(f" 清理後筆數: {cleaned_count}")
            print(f" 檔案已更新: {FILE_PATH.relative_to(BASE_DIR_get_holidays)}")
        else:
            print(f"ℹ️ 檔案中沒有發現重複的日期，無需清理。")
            
    except Exception as e:
        print(f"【重大錯誤】清理檔案時發生錯誤: {e}")

# 根據休假日 CSV 檔案，整理出該年度的非休假日（工作日）。
def get_non_holidays(file_path):
    """
    根據休假日 CSV 檔案，整理出該年度的非休假日（工作日）。
    
    :param file_path: 休假日 CSV 檔案路徑
    :return: 包含非休假日資訊 (日期, 星期, 假日說明) 的 DataFrame
    """
    
    # 1. 讀取休假日檔案
    try:
        df_holidays = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_path}")
        return None
    except Exception as e:
        print(f"讀取檔案時發生錯誤: {e}")
        return None

    # 假設休假日日期欄位名為 '日期'，且格式為 YYYY/MM/DD 或 YYYY-MM-DD
    # 請根據您的實際檔案修改欄位名稱
    date_column = '日期' 
    if date_column not in df_holidays.columns:
        print(f"錯誤：CSV 檔案中未找到欄位 '{date_column}'。請檢查欄位名稱。")
        return None

    # 將日期轉換為 datetime 物件，並處理可能的格式問題
    df_holidays[date_column] = pd.to_datetime(df_holidays[date_column], errors='coerce')
    df_holidays.dropna(subset=[date_column], inplace=True)
    
    # 2. 確定該年度的起始和結束日期
    if df_holidays[date_column].empty:
        print("錯誤：休假日資料為空或日期格式不正確。")
        return None
        
    start_year = df_holidays[date_column].min().year
    end_year = df_holidays[date_column].max().year

    # 為了簡化，我們只處理第一個偵測到的年度
    target_year = start_year
    
    # 該年度的起始日和結束日
    start_date = date(target_year, 1, 1)
    end_date = date(target_year, 12, 31)

    # 3. 建立該年度所有日期的列表
    all_dates = []
    current_date = start_date
    while current_date <= end_date:
        all_dates.append(current_date)
        current_date += timedelta(days=1)

    # 4. 建立休假日集合 (只取日期部分，忽略時間)
    holiday_set = set(df_holidays[date_column].dt.date)

    # 5. 找出非休假日
    non_holidays_data = []
    for d in all_dates:
        # 非休假日 AND 不是週末 (星期六=5, 星期日=6)
        if d not in holiday_set and d.weekday() < 5: 
            # 星期幾的中文顯示
            day_of_week = d.strftime('%a') # %a 顯示星期幾的縮寫
            
            # 假日說明欄位填寫為「工作日」或「交易日」
            description = "工作日"
            
            non_holidays_data.append({
                '日期': d.strftime('%Y-%m-%d'),
                '星期': day_of_week,
                '假日說明': description
            })

    # 6. 建立結果 DataFrame
    df_non_holidays = pd.DataFrame(non_holidays_data, columns=['日期', '星期', '假日說明'])
    
    print(f"✅ 已整理出 {target_year} 年的非休假日。")
    return df_non_holidays

# 根據休假日 CSV 檔案，整理出檔案中所有年度的非休假日（工作日），並輸出為新的 CSV 檔案。 
def get_non_holidays_all_years(file_path, output_directory=None):
    """
    根據休假日 CSV 檔案，整理出檔案中所有年度的非休假日（工作日），
    並輸出為新的 CSV 檔案。

    :param file_path: 休假日 CSV 檔案路徑
    :param output_directory: 輸出檔案的資料夾路徑 (如果為 None，則與輸入檔案在同一資料夾)
    :return: 包含所有非休假日資訊的 DataFrame (或 None if failed)
    """
    
    # 1. 讀取休假日檔案
    try:
        # 嘗試讀取檔案，建議使用 'utf-8'，如果中文亂碼可嘗試 'big5'
        df_holidays = pd.read_csv(file_path, encoding='utf-8')
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_path}")
        return None
    except Exception as e:
        print(f"讀取檔案時發生錯誤，請檢查檔案路徑和編碼: {e}")
        return None

    # 假設休假日日期欄位名為 '日期'，請依您的檔案實際名稱修改
    date_column = '日期' 
    if date_column not in df_holidays.columns:
        print(f"錯誤：CSV 檔案中未找到欄位 '{date_column}'。請檢查欄位名稱。")
        return None

    # 將日期轉換為 datetime 物件
    df_holidays[date_column] = pd.to_datetime(df_holidays[date_column], errors='coerce')
    df_holidays.dropna(subset=[date_column], inplace=True)
    
    if df_holidays[date_column].empty:
        print("錯誤：休假日資料為空或日期格式不正確。")
        return None

    # 2. 確定所有涉及的年度
    min_year = df_holidays[date_column].min().year
    max_year = df_holidays[date_column].max().year
    # 建立所有年度的範圍 (例如 2024, 2025)
    all_years = range(min_year, max_year + 1)

    # 3. 建立休假日集合 (只取日期部分，方便快速查找)
    # 此處集合包含 CSV 內所有被定義為休市的日期
    holiday_set = set(df_holidays[date_column].dt.date)

    # 星期幾的中文對照表 (0=星期一, 6=星期日)
    weekday_map = {
        0: '星期一', 1: '星期二', 2: '星期三', 3: '星期四', 
        4: '星期五', 5: '星期六', 6: '星期日'
    }

    all_non_holidays_data = []

    # 4. 針對每個年度找出非休假日 (工作日)
    print(f"開始處理年度範圍: {min_year} 年到 {max_year} 年...")
    
    for year in all_years:
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        
        current_date = start_date
        while current_date <= end_date:
            
            # 判斷是否為「非休假日」 (工作日必須滿足兩個條件)
            # 1. 必須是週一到週五 (工作日)
            # 2. 必須不在 CSV 列表中的休假日集合裡
            
            is_weekday = current_date.weekday() < 5 # 0-4 是週一到週五
            is_holiday_in_list = current_date in holiday_set
            
            if is_weekday and not is_holiday_in_list:
                day_of_week = weekday_map[current_date.weekday()]
                description = "工作日" # 假日說明欄位填寫為「工作日」
                
                all_non_holidays_data.append({
                    '日期': current_date.strftime('%Y/%m/%d'), 
                    '星期': day_of_week,
                    '假日說明': description
                })
            
            current_date += timedelta(days=1)

    # 5. 建立結果 DataFrame
    df_non_holidays = pd.DataFrame(all_non_holidays_data, columns=['日期', '星期', '假日說明'])
    
    # 6. 輸出至 CSV 檔案
    
    # 決定輸出路徑
    input_dir = os.path.dirname(file_path)
    output_path = output_directory if output_directory else input_dir
    
    # 檔案名稱格式： trading_day_YYYY-YYYY.csv
    output_filename = f'trading_day_{min_year}-{max_year}.csv'
    output_file_path = os.path.join(output_path, output_filename)
    
    # 使用 'utf-8-sig' 編碼以確保 Excel 開啟時中文不會亂碼
    df_non_holidays.to_csv(output_file_path, index=False, encoding='utf-8-sig')

    print(f"✅ 成功整理出 {min_year} 到 {max_year} 年度的非休假日 (共 {len(df_non_holidays)} 筆資料)。")
    print(f"檔案已儲存至: {output_file_path}")
    
    return df_non_holidays, min_year, max_year

# --- 主程式執行區 ---
if __name__ == '__main__':
    
    # 抓取 TWSE 休市日期，從2021年(110年開始)，因為TWSE僅提供近幾年的資料
    start_year = 2021
    end_year = datetime.datetime.today().year
    
    BASE_DIR_get_twse = pathlib.Path(__file__).resolve().parent / "datas" / "twse_holidays"
    BASE_DIR_get_holidays = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays"
    BASE_DIR_trading_day = pathlib.Path(__file__).resolve().parent / "datas" / "processed"
    
    TARGET_FILE_NAME = "holidays_all.csv"
    TWSE_DATE_COLUMN = '日期'
    
    # 根據您的要求，清理檔案名為 holidays_all.csv
    CLEAN_FILE_NAME = "holidays_all.csv" 
    # 檔案完整路徑: datas/processed/get_holidays/holidays_all.csv
    FILE_PATH = BASE_DIR_get_holidays / CLEAN_FILE_NAME
    
    for get_year in range(start_year, end_year+1):
        # ----------------------------------------------------
        # 抓取並儲存 TWSE 休市日期資料，每年一次 
        print(f"\n-----開始抓取 {get_year} 年度休市日期資料-----")

        TARGET_KEYWORD = f"twse_holidays_{get_year}"
        if check_files_for_keyword(BASE_DIR_get_twse, TARGET_KEYWORD):
            holidays_df = fetch_twse_holidays(get_year)
            save_dataframe_to_csv(holidays_df, get_year)
            
            holidays_df = transform_twse_holiday_dates(holidays_df, get_year)
            filename = pathlib.Path(os.path.dirname(os.path.abspath(__file__)), "datas", "processed" , "get_holidays" , f"twse_holidays_only_day{get_year}.csv")
            save_dataframe_to_csv(holidays_df, get_year, filename=filename)
        else:
            print(f"跳過 {get_year} 年的TWSE休市日期資料，因為已存在相關檔案。")

        #sys.exit(0) #中斷程式
        
        # 生成並取得全年度週末日期清單
        TARGET_KEYWORD = f"{get_year}_weekend_calendar"
        if check_files_for_keyword(BASE_DIR_get_holidays, TARGET_KEYWORD):
            full_year_weekend_list = generate_full_year_weekend_dates(get_year)
            save_dates_to_csv(full_year_weekend_list, get_year, filename='weekend_calendar.csv')
        else:
            print(f"跳過 {get_year} 年的全年度週末日期清單，因為已存在相關檔案。")
        
        
    print("\n--- 所有年度資料處理完成 ---")
    
        
    extracted_data_combined = pd.DataFrame()


    for file_path in BASE_DIR_get_holidays.glob("*.csv"):

        if file_path.name == TARGET_FILE_NAME:
            print(f"⏩ 略過目標儲存檔案: {TARGET_FILE_NAME}")
            continue
    # 呼叫函式

        extracted_data = extract_columns_from_second_row(file_path, columns_to_extract=[0, 1])

        extracted_data_combined = pd.concat([extracted_data_combined,extracted_data], ignore_index=True)

        print(extracted_data_combined)
        print("***"*50)
    df_sorted = extracted_data_combined.sort_values(by='日期', ascending=True).reset_index(drop=True)

    print("---- 合併後並轉換日期格式 ----")
    print(df_sorted)

    file_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__)), "datas", "processed" , "get_holidays" , 'holidays_all.csv')
        
    df_sorted.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    remove_duplicate_dates()
    

# 如果您希望輸出檔案儲存在特定資料夾，請修改 output_dir
# 預設為 None，會儲存在輸入檔案的同一資料夾
output_dir = None 

result_df, min_year, max_year = get_non_holidays_all_years(file_path, output_dir)

if result_df is not None:
    print(f"\n--- {min_year} 到 {max_year} 股票交易日整理完成 ---")
    