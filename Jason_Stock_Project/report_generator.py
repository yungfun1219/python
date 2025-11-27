import pathlib     # as pathlib
from typing import Optional, Tuple, List, Union, Dict, Any
import pandas as pd # 用於資料處理與分析
from datetime import date, datetime, timedelta, time as time_TimeClass
from dotenv import load_dotenv # ➊ 匯入函式庫

# 從 Excel 檔案中讀取股票庫存，將其另存為 CSV 檔案。
def extract_excel_sheet_filter_and_save(excel_file_path: str, sheet_name: str, filter_column: str, filter_value: any, output_dir: str = None) -> pathlib.Path:
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
    
    original_path = pathlib.Path(excel_file_path)
    
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
        output_dir = pathlib.Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
    csv_file_name = f"{sheet_name}_filtered{timestamp}.csv"
    output_csv_path = output_dir / csv_file_name
    
    # 4. 儲存為 CSV 檔案
    df.to_csv(output_csv_path, index=False, encoding='big5-sig')

    return output_csv_path

def process_and_send_stock_report(
    excel_path: str, sheet_name: str, filter_column: str, filter_value: str, 
    trading_day_file_path: pathlib.Path, base_dir: pathlib.Path, date_to_check: str,
    now_day_time: str, line_user_id: str
):
    """
    主要處理股票清單提取、數據分析、報告生成和 Line 通知發送的流程。
    """
    
    # 1. 檔案複製與提取 (可獨立為一個模組函式，提高可測試性)
    # copy_file_to_directory(SOURCE_FILE, excel_path) # 假設這部分在主程式或輔助工具中處理
    base_dir = pathlib.Path(base_dir)
    excel_path = pathlib.Path(excel_path)
    try:
        final_csv_path = extract_excel_sheet_filter_and_save(
            excel_file_path=excel_path,
            sheet_name=sheet_name,
            filter_column=filter_column,
            filter_value=filter_value,
            output_dir=None
        )
    except Exception as e:
        print(f"❌ Excel 處理失敗: {e}")
        return # 如果失敗，後續分析無法進行

    # 2. 獲取庫存股和關注股清單
    df = pd.read_csv(final_csv_path, encoding='big5', skipinitialspace=True)
    df.columns = df.columns.str.strip()
    TARGET_STOCK_NAMES = df["證券名稱"].tolist()
    
    focused_sheet_name = "關注的股票"
    focused_column_name = "證券名稱"
    focused_stock_names = get_stock_names_from_excel(excel_path, focused_sheet_name, focused_column_name)

    # 3. 獲取最近 N 個交易日
    N_DAYS = 6 
    recent_trading_days_df = find_last_n_trading_days_with_time_check(trading_day_file_path, n=N_DAYS)
    if recent_trading_days_df is None:
        print("警告：無法取得最近交易日清單，跳過個股分析。")
        return

    # 將日期格式轉換為 'YYYYMMDD' 清單
    day_roll_list = [row.replace("/", "") for row in recent_trading_days_df["日期"]]

    # 4. 生成所有股票的報告內容
    Send_message_ALL = generate_stock_report_content(
        stock_names=TARGET_STOCK_NAMES, 
        is_focused=False,
        day_roll_list=day_roll_list,
        base_dir=base_dir,
        date_to_check=date_to_check
    )
    
    Send_message_ALL += f"*****************************\n"
    Send_message_ALL += f"💡 {date_to_check} 關注股資訊💡\n"
    Send_message_ALL += f"*****************************"

    Send_message_ALL += generate_stock_report_content(
        stock_names=focused_stock_names, 
        is_focused=True,
        day_roll_list=day_roll_list,
        base_dir=base_dir,
        date_to_check=date_to_check
    )
    
    # 5. 發送 Line 通知
    # 這裡省略 Line Bot 的初始化和 Token 檢查，假設在主程式啟動時已完成
    if line_user_id:
        send_stock_notification(line_user_id, Send_message_ALL)
    else:
        print("警告：LINE_USER_ID 未設定，跳過 Line 通知發送。")
        
    print("\n--- 最終報告已列印/發送 ---")
    print(Send_message_ALL)

# 輔助函式 (將您原始程式碼中重複的邏輯提煉出來)
def generate_stock_report_content(stock_names: list, is_focused: bool, day_roll_list: list, base_dir: pathlib.Path, date_to_check: str) -> str:
    """
    生成單一類型股票 (庫存股或關注股) 的報告內容。
    (此處的內容可根據您的原始碼邏輯填寫)
    """
    # ... (包含 stock_names 迴圈、lookup_stock_price、get_day_stock_details 或 自訂價格計算/買賣超計算 的邏輯)
    report_text = ""
    # 由於篇幅限制，這裡只留下框架
    # 您的原始碼中針對庫存股和關注股有相似但略有不同的邏輯，請將其差異化處理後填入
    for stock_name in stock_names:
        # 1. 取得前一交易日價格 (作為比較基準)
        CSV_PATH_BEFORE = base_dir / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll_list[0]}_BWIBBU_d_IndexReturn.csv"
        get_price_before = lookup_stock_price(CSV_PATH_BEFORE, stock_name, "證券名稱", "收盤價")
        total_price_percent = 0
        Send_message = ""
        final_indicators = {}
        
        # 2. 迴圈處理近 N-1 天的數據
        for day_roll1 in day_roll_list[1:]:
            
            if is_focused:
                 # 關注股的價格/漲跌幅/三大法人買賣超計算
                 # 這部分與原始碼的邏輯大致相同
                 day_data = get_day_stock_details(day_roll1, stock_name, get_price_before, base_dir, "證券名稱", "收盤價")
            else:
                 # 庫存股的價格/漲跌幅/三大法人買賣超計算
                 day_data = get_day_stock_details(day_roll1, stock_name, base_dir, get_price_before, "證券名稱", "收盤價")
                 final_indicators = { 'pa_ratio': day_data['pa_ratio'], 'pe_ratio': day_data['pe_ratio'], 'pb_ratio': day_data['pb_ratio'], }

            # 彙總績效和單日訊息
            total_price_percent += day_data['price_percent']
            Send_message += f"{day_data['day_mmdd']}:{day_data['get_price']}{day_data['price_percent_formatted']}({day_data['net_volume_data']})\n"
            get_price_before = day_data['get_price']

        # 3. 格式化總體績效
        total_price_percent = round(total_price_percent, 1)
        total_price_percent_formatted = f"🔴 {abs(total_price_percent)}%" if total_price_percent > 0 else f"🟢 {abs(total_price_percent)}%"
        
        # 4. 組合報告
        prefix = "🥇" if not is_focused else "⚠️"
        report_text += f"\n= {prefix} {stock_name} 最近5日收盤價 {prefix} =\n{Send_message}"
        report_text += f"== 近5日績效:{total_price_percent_formatted} ==\n"
        
        if not is_focused:
            # 庫存股才顯示指標
            report_text += (
                f"\n--🎯【{stock_name}】個股資訊 🎯--\n"
                f"  本益比  : {final_indicators.get('pe_ratio', 'N/A')}\n"
                f"股價淨值比: {final_indicators.get('pb_ratio', 'N/A')}\n"
                f"  殖利率  : {final_indicators.get('pa_ratio', 'N/A')}%\n\n"
            )

    return report_text

# 讀取關注的股票
def get_stock_names_from_excel(file_path: str, sheet_name: str, column_name: str) -> Optional[pd.Series]:
    """
    讀取 Excel 檔案中指定工作表的指定欄位數據。
    Args:
        file_path (str): Excel 檔案的完整路徑。
        sheet_name (str): 工作表的標籤名稱 (e.g., '【關注的股票】')。
        column_name (str): 要抓取的欄位名稱 (e.g., '證券名稱')。

    Returns:
        pd.Series or None: 包含證券名稱的 Series，如果失敗則返回 None。
    """
    print(f"🔄 正在嘗試讀取 Excel 檔案：{file_path}")
    print(f"🎯 鎖定工作表：【{sheet_name}】")

    try:
        # 讀取 Excel 檔案中指定的工作表
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # 檢查欄位是否存在
        if column_name in df.columns:
            # 抓取並返回 '證券名稱' 欄位的資料
            stock_names = df[column_name]
            
            print(f"✅ 成功抓取工作表 '{sheet_name}' 中 '{column_name}' 欄位的數據。")
            
            # 輸出列表內容
            print("-" * 50)
            print("【證券名稱】列表：")
            print(stock_names.to_string(index=False)) # 輸出乾淨的列表
            print("-" * 50)
            
            return stock_names
        else:
            print(f"❌ 錯誤：工作表 '{sheet_name}' 中找不到欄位 '{column_name}'。")
            print(f"實際欄位名稱：{list(df.columns)}")
            return None

    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的 Excel 檔案路徑 -> {file_path}")
        return None
    except ValueError as e:
        if "Worksheet named" in str(e):
            print(f"❌ 錯誤：找不到名為 '{sheet_name}' 的工作表。請檢查標籤名稱是否正確。")
        else:
            print(f"❌ 讀取 Excel 檔案時發生錯誤: {e}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤: {e}")
        return None
    
    
# 從 Excel 檔案中讀取股票庫存，將其另存為 CSV 檔案。
def extract_excel_sheet_filter_and_save(excel_file_path: str, sheet_name: str, filter_column: str, filter_value: any, output_dir: str = None) -> pathlib.Path:
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
    
    original_path = pathlib.Path(excel_file_path)
    
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
        output_dir = pathlib.Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
    csv_file_name = f"{sheet_name}_filtered{timestamp}.csv"
    output_csv_path = output_dir / csv_file_name
    
    # 4. 儲存為 CSV 檔案
    df.to_csv(output_csv_path, index=False, encoding='big5')

    return output_csv_path


# 從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。
def find_last_n_trading_days_with_time_check(file_path, n=6):
    """
    從交易日檔案中，找出今天往前數 N 個交易日，並根據當前時間 (15:00) 判斷是否納入今天。

    :param file_path: 股票交易日 CSV 檔案路徑
    :param n: 往前找的交易日數量 (預設為 6)
    --取6個但最後一個不顯示，作為數據計算用
    :return: 包含最近 N 個交易日的 DataFrame (或 None if failed)
    """
    
    # 1. 定義當前時間和判斷標準
    now = datetime.now()
    today_date = now.date()
    cutoff_time = time_TimeClass(15, 0, 0) # 下午 15:00:00
    is_after_cutoff = now.time() >= cutoff_time

    print(f"當前日期: {today_date.strftime('%Y/%m/%d')}, 當前時間是否在 15:00 之後: {is_after_cutoff}")
    
    # 2. 讀取交易日檔案
    try:
        df = pd.read_csv(file_path, encoding='big5')
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

# 從指定的 CSV 檔案中查詢特定證券的收盤價。
def lookup_stock_price(file_path: pathlib.Path, stock_name: str, name_col: str, price_col: str) -> Optional[str]:
    """
    從指定的 BWIBBU CSV 檔案中查找特定股票的收盤價。
    
    修正: 確保在返回價格前，移除千位分隔符號 (,) 以避免 ValueError。
    """
    df = _read_local_csv(file_path)
    if df is None:
        print(f"  > 警告: 價格檔案 {file_path.name} 缺失或讀取失敗。")
        #sys.exit(1)  # 暫停執行，請確認日期無誤後再移除此行
        return None
        
    try:
        # 尋找匹配的股票名稱
        result = df[df[name_col] == stock_name]
        if not result.empty and price_col in result.columns:
            price_raw = result.iloc[0][price_col]

            # --- 價格清理與轉換 ---
            price_float = None
            if price_raw is not None:
                # 1. 如果是字串，移除逗號和前後空白
                if isinstance(price_raw, str):
                    price_clean_str = price_raw.replace(',', '').strip()
                else:
                    price_clean_str = str(price_raw)
                
                # 2. 嘗試轉換為 float
                try:
                    price_float = float(price_clean_str)
                except (ValueError, TypeError):
                    print(f"  > 警告: {stock_name} 的價格 '{price_raw}' 無法轉換為數字。")
                    return None
            
            if price_float is not None:
                # 返回格式化為兩位小數的價格字串
                return f"{price_float:.2f}"
            else:
                return None
        else:
            # print(f"  > 警告: 找不到 {stock_name} 的價格資料。")
            return None
    except Exception as e:
        print(f"  > 價格查詢失敗: {e}")
        return None
    
def get_day_stock_details(
    day_roll1: str,
    target_stock_name: str,
    base_dir: pathlib.Path,
    get_price_before: Optional[str],
    csv_name_column: str,
    csv_price_column: str
) -> Dict[str, Any]:
    """
    獲取單一交易日 (day_roll1) 特定股票 (target_stock_name) 的詳細資訊，
    包括收盤價、漲跌幅、三大法人買賣超、以及個股指標。
    
    Args:
        day_roll1: 當前交易日 (YYYYMMDD 格式)。
        target_stock_name: 股票名稱。
        base_dir: 專案基礎路徑。
        get_price_before: 前一交易日的收盤價 (字串或 None)。
        csv_name_column: CSV 中用於比對的股票名稱欄位名稱。
        csv_price_column: CSV 中收盤價的欄位名稱。

    Returns:
        包含當日所有處理結果的字典。
    """
    
    # ------------------ 1. 定義檔案路徑 ------------------
    
    # 價格與指標 CSV 路徑 (3_BWIBBU_d)
    csv_price_path = base_dir / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll1}_BWIBBU_d_IndexReturn.csv"
    # 三大法人買賣超 CSV 路徑 (11_T86)
    csv_volume_path = base_dir / "datas" / "raw" / "11_T86" / f"{day_roll1}_T86_InstitutionalTrades.csv"

    # 初始化結果字典
    result = {
        'day_mmdd': f"{day_roll1[4:6]}/{day_roll1[-2:]}",
        'get_price': None,
        'price_percent': 0.0,
        'price_percent_formatted': "0.0%",
        'net_volume_data': "0", # 預設為 "0" (不帶張字，後面統一加上)
        'pa_ratio': "-",
        'pe_ratio': "-",
        'pb_ratio': "-"
    }

    # ------------------ 2. 獲取收盤價 ------------------

    # 假設 lookup_stock_price 返回字串或 None
    get_price = lookup_stock_price(
        file_path=csv_price_path,
        stock_name=target_stock_name,
        name_col=csv_name_column,
        price_col=csv_price_column
    )
    result['get_price'] = get_price

    # ------------------ 3. 計算價格漲跌幅 ------------------
    
    if get_price is not None and get_price_before is not None:
        try:
            current_price = float(get_price)
            previous_price = float(get_price_before)
            
            if previous_price != 0:
                price_percent = (current_price - previous_price) / previous_price * 100
                result['price_percent'] = round(price_percent, 1)
                
                # 格式化輸出
                formatted_percent = abs(result['price_percent'])
                if price_percent > 0:
                    result['price_percent_formatted'] = f"🔴{formatted_percent}%"
                else:
                    result['price_percent_formatted'] = f"🟢{formatted_percent}%"
            else:
                print(f"⚠️ 警告: {result['day_mmdd']} 前一日價格為 0，無法計算漲跌幅。")
        except (ValueError, TypeError) as e:
            print(f"❌ 錯誤: {result['day_mmdd']} 價格轉換或計算失敗 ({e})，跳過漲跌幅。")
    else:
        # 當 get_price 或 get_price_before 為 None 時
        print(f"❌ 錯誤: {result['day_mmdd']} 價格資料缺失 (None)，跳過漲跌幅計算。")

    # ------------------ 4. 獲取三大法人買賣超 (已修正 NoneType 錯誤) ------------------
    
    # 假設 get_stock_net_volume 返回 pd.Series 或 None
    net_volume_data_series = get_stock_net_volume(csv_volume_path, target_stock_name)
    
    # >>> 關鍵修正區塊：防止 net_volume_data_series 為 None 導致 astype 錯誤
    if net_volume_data_series is not None and not net_volume_data_series.empty:
        try:
            # 1. 字串清理：將 Series 轉換為字串，移除逗號和負號 (確保只剩數字和可能的小數點)
            cleaned_volume_str = net_volume_data_series.astype(str).str.replace(',', '', regex=False).str.replace('-', '', regex=False).str.strip()
            
            # 2. 數值轉換：轉換為 float，然後除以 1000 換算成「張」
            net_volume_in_lots = pd.to_numeric(cleaned_volume_str, errors='coerce') / 1000
            
            # 3. 四捨五入並轉換為整數
            rounded_lots = net_volume_in_lots.round(0).astype('Int64') # 使用 Int64 處理 NaN/None
            
            # 4. 提取純數值並格式化
            # 由於 Series 中只有一個值，我們可以直接取第一個值（或使用 to_string，但取值更安全）
            if not rounded_lots.empty:
                volume_int = rounded_lots.iloc[0]
                
                # 格式化並儲存，使用千位分隔符
                if pd.notna(volume_int):
                    # 使用 f-string 格式化，自動加上千位分隔符
                    result['net_volume_data'] = f"{int(volume_int):,d}"
                else:
                    # 如果轉換後是 NaN/None，則設為 0
                    result['net_volume_data'] = "0"
            else:
                 # 雖然 Series 不為 empty，但數值轉換後可能為空
                 result['net_volume_data'] = "0"

        except Exception as e:
            # 捕獲所有轉換錯誤
            print(f"❌ 錯誤: {result['day_mmdd']} 買賣超資料轉換失敗 ({e})，設定為 '資料錯誤'。")
            result['net_volume_data'] = "資料錯誤"
            
    else:
        # net_volume_data_series is None 或 empty (get_stock_net_volume 失敗或找不到股票)
        print(f"找不到 {target_stock_name} 在 {result['day_mmdd']} 的買賣超股數資料。")
        result['net_volume_data'] = "0"

    # ------------------ 5. 獲取個股指標 ------------------
    
    stock_indicators_df = get_stock_indicators(csv_price_path, target_stock_name)
    
    if stock_indicators_df is not None and not stock_indicators_df.empty:
        try:
            # 確保欄位存在，並提取數據
            result['pa_ratio'] = stock_indicators_df.iloc[0]['殖利率(%)']
            result['pe_ratio'] = stock_indicators_df.iloc[0]['本益比']
            result['pb_ratio'] = stock_indicators_df.iloc[0]['股價淨值比']
        except KeyError:
            print(f"⚠️ 警告: {result['day_mmdd']} 個股指標 CSV 欄位名稱不正確或數據缺失。")
            # 保持預設的 "-"

    return result


# 讀取 CSV 檔案，篩選出指定證券名稱的數據，並返回其指標資料。
def get_stock_indicators(file_path: str, target_name: str) -> Optional[pd.DataFrame]:
    """
    讀取 CSV 檔案，篩選出指定證券名稱的數據，並返回其指標資料。
    
    Args:
        file_path (str): CSV 檔案的完整路徑。
        target_name (str): 要篩選的證券名稱 (e.g., '台玻')。
        
    Returns:
        pd.DataFrame or None: 包含目標證券指標數據的 DataFrame，如果失敗則返回 None。
    """
    #base_dir = pathlib.Path(base_dir)
    file_path = pathlib.Path(file_path)
    
    print(f"🔄 正在讀取檔案：{file_path}")
    print(f"🎯 搜尋目標證券：【{target_name}】的指標數據")

    # 1. 讀取 CSV 檔案 (多編碼嘗試)
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except:
                df = pd.read_csv(file_path, encoding='big5')
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的輸入檔案路徑 -> {file_path}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤或編碼問題：{e}")
        return None

    # 2. 檢查關鍵欄位是否存在
    # 根據 TWSE/BWIBBU 檔案常見結構，欄位名稱可能略有不同，這裡沿用上一次的欄位名稱，
    # 但請根據實際檔案情況調整，例如：'本益比' 可能為 'PE Ratio'
    required_cols = ['證券名稱', '殖利率(%)', '本益比', '股價淨值比']
    
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"⚠️ 錯誤：檔案中缺少必要的欄位：{missing_cols}。")
        print(f"檔案實際欄位名稱：{list(df.columns)}")
        # 由於 BWIBBU 檔案格式可能較為複雜，這裡先假設欄位名稱是正確的。
        # 如果執行時報錯，請檢查實際 CSV 檔案中的欄位名稱。
        return None

    # 3. 數據清理與篩選
    # 清理 '證券名稱' 兩側空白，確保精確匹配
    df['證券名稱'] = df['證券名稱'].astype(str).str.strip()

    # 篩選出目標證券名稱的數據
    target_data = df[df['證券名稱'] == target_name]

    if target_data.empty:
        print(f"\nℹ️ 提示：在檔案中找不到證券名稱為 【{target_name}】 的數據。")
        return pd.DataFrame()
    
    # 4. 提取目標欄位數據
    indicator_cols = ['證券名稱', '殖利率(%)', '本益比', '股價淨值比']
    result_df = target_data[indicator_cols].copy() 

    # 5. 輸出結果
    print(f"\n✅ 成功找到 【{target_name}】 的指標數據：")
    print("=" * 40)
    
    # 使用 to_string 進行格式化輸出
    print(
        result_df.to_string(
            index=False,
            justify='left' # 讓文字靠左對齊
        )
    )
    print("=" * 40)
    
    print("測試-------------")
    return result_df


# 讀取 CSV 檔案，篩選出指定證券名稱的資料，並只返回「買賣超股數」數據。
def get_stock_net_volume(file_path, target_name, target_column="三大法人買賣超股數"):
    """
    讀取 CSV 檔案，篩選出指定證券名稱的資料，並只返回「買賣超股數」數據。
    Args:
        file_path (str): CSV檔案的完整路徑。
        target_name (str): 要篩選的證券名稱。
        target_column (str): 要取出的欄位名稱 (預設為 '買賣超股數')。
    Returns:
        pd.Series or None: 包含目標買賣超股數的 Series，如果讀取或篩選失敗則返回 None。
    """
    print(f"🔄 正在讀取檔案：{file_path}")
    print(f"🎯 搜尋目標：【{target_name}】，並取出【{target_column}】數據")

    # 1. 讀取CSV檔案 (多編碼嘗試，確保輸入正確)
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            print("ℹ️ 成功使用 'utf-8-sig' 編碼讀取。")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                print("ℹ️ 使用 'utf-8' 編碼讀取。")
            except:
                df = pd.read_csv(file_path, encoding='big5')
                print("ℹ️ 使用 'big5' 編碼讀取。")
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的輸入檔案路徑 -> {file_path}")
        return None
    except Exception as e:
        print(f"❌ 發生其他錯誤或編碼問題：{e}")
        return None
    
    # 2. 檢查關鍵欄位是否存在
    required_cols = ['證券名稱', target_column]
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"⚠️ 錯誤：檔案中缺少必要的欄位：{missing_cols}。")
        print(f"檔案實際欄位名稱：{list(df.columns)}")
        return None

    # 3. 數據清理與篩選
    # 清理 '證券名稱' 兩側空白，確保精確匹配
    df['證券名稱'] = df['證券名稱'].astype(str).str.strip()

    # ⭐ 核心修改點 A: 清理 '買賣超股數' 欄位，移除引號並清理空白，為數值轉換做準備
    try:
        df[target_column] = df[target_column].astype(str).str.replace('"', '', regex=False).str.strip()
        # print(f"✅ 成功移除 {target_column} 欄位中的雙引號。")
    except Exception as e:
        print(f"⚠️ 警告：嘗試清理 {target_column} 時發生錯誤：{e}")

    target_data = df[df['證券名稱'] == target_name]

    # 4. 取出目標欄位數據
    if target_data.empty:
        print(f"\nℹ️ 提示：在檔案中找不到證券名稱為 【{target_name}】 的數據。")
        # 統一返回 None，讓外部程式碼只需檢查 None
        return None
    else:
        # 取出 '買賣超股數' 欄位，這是一個 pandas.Series 對象
        net_volume_series = target_data[target_column]
        
        print(f"\n✅ 成功找到 【{target_name}】 的 {len(net_volume_series)} 筆【{target_column}】數據。")
        print("-" * 60)
        # 這裡不顯示 Series 原始內容，讓最終輸出更聚焦
        
    #print("測試1",net_volume_series)
    #sys.exit(1)  # 暫停執行，請確認日期無誤後再移除此行
        
    return net_volume_series

def send_stock_notification(user_id, message_text):
        try:
            push_message_request = PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=message_text)]
            )
            # 注意：這裡使用全域變數 messaging_api，如果初始化失敗，這裡會報錯
            messaging_api.push_message(push_message_request) 
            print(f"訊息已成功發送給 {user_id}")
        except Exception as e:
            print(f"其他錯誤: {e}")
            
def _read_local_csv(file_path: pathlib.Path) -> Optional[pd.DataFrame]:
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