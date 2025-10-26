import pathlib
import pandas as pd
import requests
import os
from io import StringIO
from typing import Optional
import datetime
import re

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

# 將日期字串轉換為中文的星期幾。
def get_day_of_week_from_string(date_string: str, date_format: str = '%Y/%m/%d') -> str:
    """
    將日期字串轉換為中文的星期幾。

    參數:
        date_string (str): 日期字串，例如 '2024/05/01'。
        date_format (str): 日期字串的格式，預設為 '%Y/%m/%d'。

    返回:
        str: 中文的星期幾 (例如 '星期三')。
    """
    
    # 定義中文星期幾的對應列表 (datetime.weekday() 返回 0-6，0 代表星期一)
    chinese_weekdays = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日']
    
    try:
        # 1. 將日期字串解析為 datetime 物件
        date_obj = datetime.strptime(date_string, date_format)
        
        # 2. 取得星期幾的數字 (0=星期一, 6=星期日)
        day_index = date_obj.weekday()
        
        # 3. 透過索引取得中文星期幾
        return chinese_weekdays[day_index]
        
    except ValueError as e:
        return f"日期格式錯誤或無效的日期: {e}"

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

# 定義一個轉換函式 (此函式在主程式中已不再使用，但保留以防未來需要)
def format_chinese_date(md_str: str) -> str:
    """將 MM/DD 格式字串轉換為中文 M月D日，並移除前導零。"""
    if pd.isna(md_str):
        return ''
    try:
        # 假設輸入是 MM/DD，例如 '02/09'
        month, day = md_str.split('/')
        # 使用 int() 轉換來移除前導零 (e.g., '02' -> 2)
        return f"{int(month)}月{int(day)}日"
    except ValueError:
        return md_str # 如果格式錯誤，則返回原始字串

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


# --- 主程式執行區 ---
if __name__ == '__main__':
    
    # 抓取 TWSE 休市日期，從2021年(110年開始)，因為TWSE僅提供近幾年的資料
    start_year = 2021
    end_year = datetime.datetime.today().year
    
    BASE_DIR_get_twse = pathlib.Path(__file__).resolve().parent / "datas" / "twse_holidays"
    BASE_DIR_get_holidays = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays"
    
    for get_year in range(start_year, end_year+1):
        # ----------------------------------------------------
        # 抓取並儲存 TWSE 休市日期資料，每年一次 
        print(f"\n-----開始抓取 {get_year} 年度休市日期資料-----")

        TARGET_KEYWORD = f"twse_holidays_{get_year}"
        if check_files_for_keyword(BASE_DIR_get_twse, TARGET_KEYWORD):
            holidays_df = fetch_twse_holidays(get_year)
            save_dataframe_to_csv(holidays_df, get_year)
            
            holidays_df = transform_twse_holiday_dates(holidays_df, get_year)
            #filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datas", "processed" , "get_holidays" , f"twse_holidays_only_day{get_year}.csv")
            filename = pathlib.Path(os.path.dirname(os.path.abspath(__file__)), "datas", "processed" , "get_holidays" , f"twse_holidays_only_day{get_year}.csv")
            save_dataframe_to_csv(holidays_df, get_year, filename=filename)
        else:
            print(f"跳過 {get_year} 年的TWSE休市日期資料，因為已存在相關檔案。")

        
        # 生成並取得全年度週末日期清單
        TARGET_KEYWORD = f"{get_year}_weekend_calendar"
        if check_files_for_keyword(BASE_DIR_get_holidays, TARGET_KEYWORD):
            full_year_weekend_list = generate_full_year_weekend_dates(get_year)
            save_dates_to_csv(full_year_weekend_list, get_year, filename='weekend_calendar.csv')
        else:
            print(f"跳過 {get_year} 年的全年度週末日期清單，因為已存在相關檔案。")
        
    print("\n--- 所有年度資料處理完成 ---")

    extracted_data_combined = pd.DataFrame()

    #BASE_DIR = pathlib.Path(r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\processed\get_holidays")
    TARGET_FILE_NAME = "holidays_all.csv"
    TWSE_DATE_COLUMN = '日期'
    

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
    