import pandas as pd
import pathlib
from pathlib import Path
import os
from datetime import date, datetime, time, timedelta

# 從指定的 CSV 檔案中查詢特定證券的收盤價。
def lookup_stock_price(file_path: str, stock_name: str, name_col: str, price_col: str):
    """
    從指定的 CSV 檔案中查詢特定證券的收盤價。
    """
    file = Path(file_path)
    
    #print(f"✅ 正在嘗試讀取檔案: {file.name}")
    #print(f"🔍 查詢目標: {stock_name}")
    
    if not file.exists():
        print(f"❌ 錯誤：找不到檔案在路徑：{file_path}")
        return

    try:
        # 讀取 CSV 檔案，使用 Big5 編碼 (臺灣金融數據常用)，並清理欄位名稱的空白
        df = pd.read_csv(file_path, encoding='utf-8', skipinitialspace=True)
        df.columns = df.columns.str.strip()
        
        # 檢查關鍵欄位是否存在
        if name_col not in df.columns or price_col not in df.columns:
            print(f"❌ 錯誤：CSV 檔案中缺少必要的欄位 ('{name_col}' 或 '{price_col}')。")
            return

        # 確保比對欄位是字串且清理空白
        df[name_col] = df[name_col].astype(str).str.strip()

        # 執行篩選
        result = df[df[name_col] == stock_name]

        if result.empty:
            print(f"\n⚠️ 警告：在檔案中找不到 '{stock_name}' 的收盤價資料。")
            return

        # 取得收盤價，只取第一個結果（因為可能有多行相同名稱，但通常只取第一筆）
        price = result.iloc[0][price_col]
        
        # print("\n" + "="*50)
        # print(f"🎉 查詢結果 ({file.name})")
        # print(f"證券名稱: {stock_name}")
        # print(f"收盤價 ({price_col}): **{price}**")
        # print("="*50)
        return price    
    except Exception as e:
        print(f"❌ 讀取或處理檔案時發生錯誤：{e}")

# 從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。
def find_last_n_trading_days_with_time_check(file_path, n=5):
    """
    從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。

    :param file_path: 股票交易日 CSV 檔案路徑
    :param n: 往前找的交易日數量 (預設為 5)
    :return: 包含最近 N 個交易日的 DataFrame (或 None if failed)
    """
    
    # 1. 定義當前時間和判斷標準
    now = datetime.now()
    today_date = now.date()
    cutoff_time = time(15, 0, 0) # 下午 15:00:00
    is_after_cutoff = now.time() >= cutoff_time

    print(f"當前日期: {today_date.strftime('%Y/%m/%d')}, 當前時間是否在 15:00 之後: {is_after_cutoff}")
    
    # 2. 讀取交易日檔案
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_path}")
        return None
    except Exception as e:
        print(f"讀取檔案時發生錯誤，請檢查檔案路徑和編碼: {e}")
        return None

    # 假設日期欄位為 '日期'
    date_column = '日期' 
    if date_column not in df.columns:
        # 嘗試使用常見的英文欄位名
        if 'Date' in df.columns:
            date_column = 'Date'
        else:
            print(f"錯誤：無法識別交易日期的欄位名稱。請檢查您的 CSV 檔案。")
            return None
        
    # 3. 清理和轉換日期格式
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.normalize()
    df.dropna(subset=[date_column], inplace=True)
    
    # 建立所有交易日的集合，用於快速判斷今天是否為交易日
    all_trading_dates = set(df[date_column].dt.date)
    is_today_trading_day = today_date in all_trading_dates
    
    print(f"今天 ({today_date.strftime('%Y/%m/%d')}) 是否為交易日: {is_today_trading_day}")

    # 4. 根據時間判斷決定資料篩選的截止日期
    
    # 預設：如果不滿足納入今天的條件，則截止日期為昨天
    inclusion_date = today_date - timedelta(days=1)
    
    # 判斷是否應該納入今天
    if is_today_trading_day and is_after_cutoff:
        # 條件 1: 今天是交易日
        # 條件 2: 且時間在 15:00 之後 (視為今天交易已完成)
        # -> 納入今天
        inclusion_date = today_date
        print("-> 判斷：納入今天的交易日。")
    else:
        # 其他情況 (非交易日、或交易日但未滿 15:00)
        # -> 排除今天，只取昨天及更早的交易日
        inclusion_date = today_date - timedelta(days=1)
        print("-> 判斷：排除今天的交易日，只取昨天及更早的日期。")

    # 5. 篩選、排序並選取最近 N 個交易日
    
    # 篩選出日期小於或等於決定截止日期的交易日
    df_past = df[df[date_column].dt.date <= inclusion_date]
    
    # 確保日期由近到遠排序
    df_past = df_past.sort_values(by=date_column, ascending=False)

    # 選取最近的 N 個交易日
    last_n_days = df_past.head(n)

    if last_n_days.empty:
        print(f"警告：交易日資料不足，無法找到往前 {n} 個交易日。")
        return None

    # 將結果由舊到新排序並格式化輸出
    last_n_days = last_n_days.sort_values(by=date_column, ascending=True)
    last_n_days[date_column] = last_n_days[date_column].dt.strftime('%Y/%m/%d')
    
    print(f"\n✅ 成功找到今天往前 {n} 個交易日。")
    return last_n_days

# 從 Excel 檔案中讀取股票庫存，將其另存為 CSV 檔案。
def extract_excel_sheet_filter_and_save(excel_file_path: str, sheet_name: str, filter_column: str, filter_value: any, output_dir: str = None) -> Path:
    """
    從指定的 Excel 檔案中讀取特定工作表，跳過第二行，篩選資料後，將其另存為 CSV 檔案。

    Args:
        excel_file_path (str): 原始 Excel 檔案的完整路徑。
        sheet_name (str): 要讀取的工作表名稱 (例如: '股票庫存統計')。
        filter_column (str): 要進行篩選的欄位名稱 (例如: '目前股數庫存統計')。
        filter_value (any): 要篩除的值。
        output_dir (str, optional): CSV 檔案的儲存目錄。如果為 None，則儲存在原始檔案的目錄。

    Returns:
        Path: 儲存成功的 CSV 檔案路徑。
    """
    
    original_path = Path(excel_file_path)
    
    if not original_path.exists():
        raise FileNotFoundError(f"錯誤：找不到 Excel 檔案在路徑：{excel_file_path}")

    print(f"✅ 正在讀取 Excel 檔案：{original_path.name}")
    print(f"🎯 目標工作表名稱：{sheet_name}")

    try:
        # 1. 讀取 Excel 中指定工作表的資料
        # header=0: 指定 Excel 的第一行（索引 0）作為欄位名稱
        # skiprows=[1]: 跳過索引為 1 的行，即 Excel 中的第二行
        df = pd.read_excel(
            original_path, 
            sheet_name=sheet_name, 
            header=0,
            skiprows=[1]  # <--- ❗ 這裡加入跳過 Excel 第二行（索引 1）的設定
        )
        
        if df.empty:
            print(f"警告：工作表 '{sheet_name}' 讀取到的數據為空。")
            return None

    except ValueError as e:
        raise ValueError(f"錯誤：在 Excel 檔案中找不到名為 '{sheet_name}' 的工作表。請檢查名稱是否正確。詳細錯誤: {e}")
    except Exception as e:
        raise Exception(f"讀取 Excel 檔案時發生錯誤：{e}")
        
    # 2. **【關鍵篩選步驟】**
    if filter_column not in df.columns:
        print(f"警告：找不到篩選欄位 '{filter_column}'。跳過篩選步驟。")
    else:
        initial_rows = len(df)
        print(f"\n🔍 開始篩選：移除 '{filter_column}' 值為 '{filter_value}' 的資料...")
        
        # 嘗試將篩選欄位轉換為數值類型，coerce 會將非數值轉換為 NaN
        df[filter_column] = pd.to_numeric(df[filter_column], errors='coerce')
        
        # 篩選邏輯：保留 '目前股數庫存統計' 不等於 0 的行
        df_filtered = df[df[filter_column] != float(filter_value)]
        
        removed_rows = initial_rows - len(df_filtered)
        print(f"  -> 原始筆數 (已跳過第二行): {initial_rows} 筆")
        print(f"  -> 移除筆數: {removed_rows} 筆")
        print(f"  -> 剩餘筆數: {len(df_filtered)} 筆")
        
        df = df_filtered
        
        if df.empty:
            print("警告：篩選後數據為空。")
            return None


    # 3. 準備輸出 CSV 檔案的路徑
    
    if output_dir is None:
        output_dir = original_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
    csv_file_name = f"{sheet_name}_filtered{timestamp}.csv"
    output_csv_path = output_dir / csv_file_name
    
    # 4. 儲存為 CSV 檔案
    df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

    return output_csv_path


# ==========================================================
# --- 參數設定 ---
# ==========================================================

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
EXCEL_PATH = BASE_DIR + "/datas/股票分析.xlsx"
SHEET_NAME = "股票庫存統計"
FILTER_COLUMN = "目前股數庫存統計"
FILTER_VALUE = "0"
OUTPUT_DIRECTORY = None 

# --- 主要執行區塊 ---
try:
    final_csv_path = extract_excel_sheet_filter_and_save(
        excel_file_path=EXCEL_PATH,
        sheet_name=SHEET_NAME,
        filter_column=FILTER_COLUMN,
        filter_value=FILTER_VALUE,
        output_dir=OUTPUT_DIRECTORY
    )
    
    if final_csv_path:
        print("\n" + "="*50)
        print("🎉 任務成功完成！")
        print(f"CSV 檔案已儲存至：\n {final_csv_path}")
        print("="*50)

except (FileNotFoundError, ValueError, Exception) as e:
    print("\n" + "="*50)
    print("❌ 程式執行失敗！")
    print(e)
    print("="*50)

#-- 取得證券名稱清單 ---
print("\n--- 取得證券名稱清單 ---")    
df = pd.read_csv(final_csv_path, encoding='utf-8', skipinitialspace=True)
df.columns = df.columns.str.strip()

#print(df["證券名稱"])
TARGET_STOCK_NAMES = []
for col in df["證券名稱"]:
    TARGET_STOCK_NAMES.append(col)

#-- 取得往前5個交易日 ---
file_path = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / "trading_day_2021-2025.csv"

N_DAYS = 5 # 往前找的交易日數量

recent_trading_days_df = find_last_n_trading_days_with_time_check(file_path, n=N_DAYS)




for TARGET_STOCK_NAME in TARGET_STOCK_NAMES:
#    print(f"\n--- {TARGET_STOCK_NAME} 最近 5 個交易日的收盤價 ---")
    Send_message = ""
    #-- 取得五個交易日的收盤價並合併 ---
    #TARGET_STOCK_NAME = "台玻" 
    CSV_NAME_COLUMN = "證券名稱" # 假設 CSV 中用於名稱比對的欄位
    CSV_PRICE_COLUMN = "收盤價"  # 假設 CSV 中收盤價的欄位

    day_roll = []
    for row in recent_trading_days_df["日期"]:
        TARGET_DATE = row.replace("/", "")
        day_roll.append(TARGET_DATE)

    BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

    if recent_trading_days_df is not None:
        print(f"\n--{TARGET_STOCK_NAME}最近5個交易日--")

    for day_roll1 in day_roll:
        CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll1}_BWIBBU_d_IndexReturn.csv"

        get_price = lookup_stock_price(
            file_path=CSV_PATH,
            stock_name=TARGET_STOCK_NAME,
            name_col=CSV_NAME_COLUMN,
            price_col=CSV_PRICE_COLUMN
        )
        Send_message += f"{day_roll1} 收盤價: {get_price}\n"
    print(Send_message)    

#print(Send_message)