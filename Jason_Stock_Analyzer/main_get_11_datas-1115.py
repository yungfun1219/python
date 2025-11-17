#標準函式庫
import os
import sys
import re
import shutil
from io import StringIO
import pathlib     # as pathlib
from datetime import date, datetime, timedelta, time as time_TimeClass

#第三方函式庫
import numpy as np # 用於數值操作
import pandas as pd # 用於資料處理與分析
import requests
import schedule
import keyboard  # 用於監聽鍵盤事件
from dotenv import load_dotenv # ➊ 匯入函式庫
from typing import Optional, Tuple, List, Union
import time as time_module # 用於 sleep() 或 time()

#本地模組
import get_stocks_company_all 
from utils import jason_utils as jutils

# 抑制當 verify=False 時彈出的 InsecureRequestWarning 警告
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# ==========================================================
# 參數設定  --- 配置 (Configuration) ---
# ==========================================================
SUMMARY_LOG_FILENAME_PREFIX = "fetch_summary" # 定義摘要日誌檔案前綴

# 設定鍵盤監控 -- 1. 初始化運行狀態 (確保是全局變數)
running = True
# """按鍵 [Q] 停止程式"""
def stop_program():
    print("\n\n👋 偵測到 'Q' 鍵，程式即將安全退出...")
    global running
    running = False
    
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
    
    return result_df

# 三大法人買超前20
def get_top_20_institutional_trades_filtered(
    file_path: str, 
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號"
) -> Optional[pd.DataFrame]:
    """
    讀取 CSV 檔案，進行以下篩選：
    1. 證券代號必須為 4 位數字。
    2. 三大法人買賣超股數必須為正數 (買超)。
    3. 返回買賣超股數最大的前 10 名數據，並輸出為格式化表格。
    """
    print(f"\n🔄 正在讀取檔案：{file_path}")
    print(f"🎯 篩選條件：1. 代號為 4 位數字 | 2. 買賣超股數 > 0")

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
    required_cols = [volume_column, code_column, '證券名稱']
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"⚠️ 錯誤：檔案中缺少必要的欄位：{missing_cols}。")
        return None

    # 3. 數據清理與數值轉換
    try:
        # 清理買賣超股數欄位：移除引號和逗號
        df[volume_column] = (
            df[volume_column].astype(str).str.replace('"', '', regex=False)
            .str.replace(',', '', regex=False).str.strip()
        )
        # 轉換為數值類型，無法轉換的值設為 NaN
        df[volume_column] = pd.to_numeric(df[volume_column], errors='coerce')
        
        # 清理證券代號欄位
        df[code_column] = df[code_column].astype(str).str.strip()
        
        # 移除無法轉換為數值的行
        df.dropna(subset=[volume_column], inplace=True)
        
    except Exception as e:
        print(f"❌ 數據清理或數值轉換失敗：{e}")
        return None

    # 4. 執行篩選條件 1：證券代號為 4 位數字
    # 使用正則表達式篩選出完全符合四位數字的代號
    df_filtered_code = df[df[code_column].str.match(r'^\d{4}$')]
    
    if df_filtered_code.empty:
        print("ℹ️ 提示：篩選後，沒有找到證券代號為 4 位數字的數據。")
        return pd.DataFrame()

    # 5. 執行篩選條件 2：買賣超股數為正數 (買超)
    df_filtered_positive = df_filtered_code[df_filtered_code[volume_column] > 0]

    if df_filtered_positive.empty:
        print("ℹ️ 提示：篩選後，沒有找到三大法人買超 (正數) 的數據。")
        return pd.DataFrame()

    # 6. 排序並取出前 20 名
    df_sorted = df_filtered_positive.sort_values(
        by=volume_column, 
        ascending=False # 買超數最大的排在最前面
    )
    
    # 取出前 20 筆數據
    top_20_trades = df_sorted.head(20)

    # 7. 輸出結果 (固定欄位寬度與置中對齊)
    
    print(f"\n✅ 篩選後的三大法人買超前 {len(top_20_trades)} 名：")
    print("=" * 40)
    
    # 格式化輸出：將股數轉換為整數格式，並加上千分位逗號
    top_20_trades_display = top_20_trades.copy()
    
    # 重新命名欄位以簡化標題
    volume_col_display_name = '買超張數'
    top_20_trades_display = top_20_trades_display.rename(
        columns={'證券代號': '代號', volume_column: volume_col_display_name}
    )

    # 格式化數字 (加上千分位逗號)
    top_20_trades_display[volume_col_display_name] = top_20_trades_display[volume_col_display_name].apply(lambda x: f"{int(x/1000):,}")

    # 定義輸出的欄位順序
    #actual_display_cols = ['代號', '證券名稱', volume_col_display_name]
    actual_display_cols = ['證券名稱', volume_col_display_name]

    # 設定每個欄位的最小寬度，以利置中 (中文字佔 2 寬度)
    col_space_width = 8 

    # 使用 to_string 配合 col_space 和 justify='center'
    print(
        top_20_trades_display[actual_display_cols].to_string(
            index=False,
            col_space=col_space_width, 
            justify='left' # 嘗試置中對齊
        )
    )
    print("=" * 40)
    
    top_20_trades = ""
    index_no = 1
    target_length = 6
    for index, rol in top_20_trades_display.iterrows():
        index_no_str = str(index_no).zfill(2)
        
        current_length = len(rol['證券名稱'].strip())
        
        current_volume_column = rol['證券名稱'].strip()
        if current_length <= target_length:
        # 計算需要填充的長度
            padding_needed = target_length - current_length 
            new_current_volume_column = current_volume_column.ljust(padding_needed, ' ')
        
        top_20_trades += f"{index_no_str}." + f" {new_current_volume_column} " + f" ({rol[volume_col_display_name]}張)\n"
        index_no += 1
        
    return top_20_trades

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
        return pd.Series(dtype='object')
    else:
        # 取出 '買賣超股數' 欄位，這是一個 pandas.Series 對象
        net_volume_series = target_data[target_column]
        
        print(f"\n✅ 成功找到 【{target_name}】 的 {len(net_volume_series)} 筆【{target_column}】數據。")
        print("-" * 60)
        # 這裡不顯示 Series 原始內容，讓最終輸出更聚焦
        
    return net_volume_series

# 讀取指定的CSV檔案，選取第2欄到第6欄的數據
def select_and_save_columns_fix_encoding(input_file_path, output_directory, output_file_name="selected_data.csv"):
    """
    讀取指定的CSV檔案，選取第2欄到第6欄的數據，
    並將結果另存為新的CSV檔案，使用 'utf-8-sig' 編碼解決輸出中文亂碼問題。

    Args:
        input_file_path (str): 來源 CSV 檔案的完整路徑。
        output_directory (str): 儲存新 CSV 檔案的目錄路徑。
        output_file_name (str): 儲存新 CSV 檔案的名稱。

    Returns:
        bool: 成功儲存則返回 True，否則返回 False。
    """
    # 組合完整的輸出路徑
    output_file_path = os.path.join(output_directory, output_file_name)
    
    # 確保輸出目錄存在
    os.makedirs(output_directory, exist_ok=True)
    
    print(f"🔄 正在讀取檔案：{input_file_path}")

    try:
        # 1. 讀取CSV檔案 (保留多編碼嘗試，確保輸入正確)
        try:
            df = pd.read_csv(input_file_path, encoding='big5')
            print("ℹ️ 成功使用 'big5' 編碼讀取檔案。")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(input_file_path, encoding='utf-8')
                print("ℹ️ 成功使用 'utf-8' 編碼讀取檔案。")
            except Exception:
                df = pd.read_csv(input_file_path, encoding='cp950')
                print("ℹ️ 成功使用 'cp950' 編碼讀取檔案。")
                
        # 2. 判斷欄位數量是否足夠，並選取第 2 欄到第 6 欄 (索引 1 到 5)
        start_index = 1 
        end_index = 6
        num_columns = len(df.columns)
        
        if num_columns < end_index:
            print(f"⚠️ 錯誤：檔案欄位總數不足 {end_index} 欄 (只有 {num_columns} 欄)。無法選取第 2 到 6 欄。")
            return False

        # 選取數據
        df_selected = df.iloc[:, start_index:end_index]
        
        print(f"✅ 成功選取欄位：{list(df_selected.columns)}")

        # --- 核心修改點 ---
        # 3. 另存為新的 CSV 檔案，使用 'utf-8-sig' 編碼
        # 'utf-8-sig' 包含 BOM (Byte Order Mark)，有助於 Excel 等軟體正確識別中文檔頭。
        df_selected.to_csv(output_file_path, index=False, encoding='utf-8-sig') 
        # -------------------

        print("-" * 40)
        print(f"✨ 數據已成功儲存到：{output_file_path}")
        print("-" * 40)
        return True

    except FileNotFoundError:
        print(f"❌ 錯誤：找不到指定的輸入檔案路徑 -> {input_file_path}")
        return False
    except pd.errors.EmptyDataError:
        print(f"❌ 錯誤：檔案是空的或無效的數據格式 -> {input_file_path}")
        return False
    except Exception as e:
        print(f"❌ 發生其他錯誤：{e}")
        return False

# 將指定檔案複製到目標目錄。
def copy_file_to_directory(source: str, destination: str):
    """
    將指定檔案複製到目標目錄。
    """
    source_path = pathlib.Path(source)
    destination_dir_path = pathlib.Path(destination)
    
    print(f"✅ 正在準備複製檔案...")
    print(f"來源: {source_path}")
    print(f"目標目錄: {destination_dir_path}")

    # 1. 檢查來源檔案是否存在
    if not source_path.exists() or not source_path.is_file():
        print(f"❌ 錯誤：找不到來源檔案或來源不是一個檔案: {source}")
        return

    # 2. 檢查目標目錄是否存在 (如果不存在，shutil.copy 會自動創建，但我們最好先檢查並列印訊息)
    if not destination_dir_path.exists():
        print(f"⚠️ 警告：目標目錄 '{destination}' 不存在，正在嘗試建立...")
        try:
            destination_dir_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"❌ 錯誤：無法建立目標目錄: {e}")
            return
    
    try:
        # 3. 執行複製操作
        # shutil.copy 會將檔案複製到目錄中，並保留原始檔案名
        shutil.copy(source, destination)
        
        # 建立完整的目標路徑用於輸出訊息
        destination_file_path = destination_dir_path / source_path.name
        
        print("\n" + "="*50)
        print("🎉 檔案複製成功！")
        print(f"新檔案位置: {destination_file_path}")
        print("="*50)

    except Exception as e:
        print(f"❌ 複製檔案時發生錯誤: {e}")

# 從指定的 CSV 檔案中查詢特定證券的收盤價。
def lookup_stock_price(file_path: str, stock_name: str, name_col: str, price_col: str):
    """
    從指定的 CSV 檔案中查詢特定證券的收盤價。
    """
    file = pathlib.Path(file_path)
    
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
    df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

    return output_csv_path

# 讀取 CSV 檔案，篩選出在今天或今天之前的所有日期，並以 YYYYMMDD 字串格式返回。
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
    BASE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
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

# 檢查是否為交易日，若是則回傳True，否則回傳"False"，下一個交易日
def check_next_date_in_csv(file_path: str, date_to_check: str, date_column_name: str = '日期') -> Union[bool, pd.Series]:
    """
    檢查指定日期字串是否出現在 CSV 檔案的特定欄位中。
    Args:
        file_path (str): holidays_all.csv 檔案的完整路徑。
        date_to_check (str): 要檢查的日期字串，例如 '2025/10/10'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[bool, pd.Series]: 如果找到，返回包含匹配行的 Series (布林值)，
                                如果未找到或檔案不存在，返回 False。返回下個交易日的 Series (布林值)，
    """
    #print(f"🔍 正在檢查檔案: {os.path.basename(file_path)}")
    #print(f"目標日期: {date_to_check}")

    if not os.path.exists(file_path):
        print("【錯誤】檔案路徑不存在，請確認路徑是否正確。")
        return False, None
        
    try:
        # 且您儲存時使用 encoding='utf-8-sig'，這裡也使用相同的編碼讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            print(f"檔案中的欄位有: {df.columns.tolist()}")
            return False, None
        
        # 使用向量化操作 (isin) 檢查欄位中是否包含目標日期
        # 即使欄位類型是 object (字串)，也能正確檢查
        
        date_format = '%Y/%m/%d'
        current_date = datetime.strptime(date_to_check, date_format)
        one_day = timedelta(days=1)
        date_to_check_save = date_to_check
        check_next_day = True
        while check_next_day:
            
            is_present = df[date_column_name].isin([date_to_check])
            #print("測試1:date_column_name:", date_to_check)
            
            if is_present.any():
                # 找到匹配的行
                matched_rows = df[is_present]
                print(f"✅ 日期 '{date_to_check}' 為交易日！")
                #print("--- 匹配的資料列 ---")
                #print(matched_rows)
                check_next_day = False
            
            else:
                print(f"✅ 日期 '{date_to_check}' 休市日！")
                tomorrow_date = current_date + one_day
                tomorrow_date_str = tomorrow_date.strftime(date_format)
                #print("測試2:date_column_name:", tomorrow_date_str)    
                date_to_check = tomorrow_date_str
                current_date = tomorrow_date
                check_next_day = True
                
        if date_to_check_save == current_date.strftime(date_format):
            print(f"今天日期: {date_to_check_save} 為交易日")
            return True, date_to_check_save
        else:
            print(f"今天日期: {date_to_check_save} 為休市日")
            print(f"下一個交易日: {current_date.strftime(date_format)}")
            return False, current_date.strftime(date_format)
        
    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return False, None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return False, None

# 檢查是否為交易日，若是則回傳True，否則回傳"False"，上一個交易日
def check_pre_date_in_csv(file_path: str, date_to_check: str, date_column_name: str = '日期') -> Union[bool, pd.Series]:
    """
    檢查指定日期字串是否出現在 CSV 檔案的特定欄位中。
    Args:
        file_path (str): holidays_all.csv 檔案的完整路徑。
        date_to_check (str): 要檢查的日期字串，例如 '2025/10/10'。
        date_column_name (str): 檔案中包含日期的欄位名稱，預設為 '日期'。
    Returns:
        Union[bool, pd.Series]: 如果找到，返回包含匹配行的 Series (布林值)，
                                如果未找到或檔案不存在，返回 False。返回下個交易日的 Series (布林值)，
    """
    #print(f"🔍 正在檢查檔案: {os.path.basename(file_path)}")
    #print(f"目標日期: {date_to_check}")

    if not os.path.exists(file_path):
        print("【錯誤】檔案路徑不存在，請確認路徑是否正確。")
        return False, None
        
    try:
        # 且您儲存時使用 encoding='utf-8-sig'，這裡也使用相同的編碼讀取
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        if date_column_name not in df.columns:
            print(f"【錯誤】檔案中找不到指定的日期欄位: '{date_column_name}'。")
            print(f"檔案中的欄位有: {df.columns.tolist()}")
            return False, None
        
        # 使用向量化操作 (isin) 檢查欄位中是否包含目標日期
        # 即使欄位類型是 object (字串)，也能正確檢查
        
        date_format = '%Y/%m/%d'
        current_date = datetime.strptime(date_to_check, date_format)
        one_day = timedelta(days=1)
        date_to_check_save = date_to_check
        check_next_day = True
        while check_next_day:
            
            is_present = df[date_column_name].isin([date_to_check])
            #print("測試1:date_column_name:", date_to_check)
            
            if is_present.any():
                # 找到匹配的行
                matched_rows = df[is_present]
                print(f"✅ 日期 '{date_to_check}' 為交易日！")
                #print("--- 匹配的資料列 ---")
                #print(matched_rows)
                check_next_day = False
            
            else:
                print(f"✅ 日期 '{date_to_check}' 休市日！")
                tomorrow_date = current_date - one_day
                tomorrow_date_str = tomorrow_date.strftime(date_format)
                #print("測試2:date_column_name:", tomorrow_date_str)    
                date_to_check = tomorrow_date_str
                current_date = tomorrow_date
                check_next_day = True
                
        if date_to_check_save == current_date.strftime(date_format):
            print(f"今天日期: {date_to_check_save} 為交易日")
            return True, date_to_check_save
        else:
            print(f"今天日期: {date_to_check_save} 為休市日")
            print(f"上一個交易日: {current_date.strftime(date_format)}")
            return False, current_date.strftime(date_format)
        
    except pd.errors.EmptyDataError:
        print("【錯誤】檔案內容為空。")
        return False, None
    except Exception as e:
        print(f"【錯誤】讀取或處理檔案時發生錯誤: {e}")
        return False, None



# 設定您想要抓取的目標日期 (只需修改此處即可抓取所有報告的資料)
def main_run():
    #----------------
    global running # 引用全局變數
    
    if not running:
        # 如果在等待執行的排程隊列中，檢查到不運行，則直接跳過
        print("\n[定時任務]: 偵測到退出信號，跳過本次執行。")
        return

    #--------------
    TARGET_DATE = date.today().strftime("%Y%m%d") 
    Yesterday_day = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    Now_time_hour = datetime.now().strftime("%H")  #取得目前系統時間的「幾點鐘」
    Now_day_time = datetime.now().strftime("%Y-%m-%d %H:%M")  #取得目前系統時間的日期及時間「例如 2025-11-12 11:12」
    Now_time_year = datetime.now().strftime("%Y")  #取得目前系統時間的「年」
    Trading_day_file_path = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-{Now_time_year}.csv"
    DATE_TO_CHECK = date.today().strftime("%Y/%m/%d")  
    
    # 處理要抓取哪一天的資料邏輯
    result_found_next = check_next_date_in_csv(Trading_day_file_path, DATE_TO_CHECK)
    if result_found_next[0]:  # 如果今天是交易日
        TARGET_DATE = DATE_TO_CHECK  # 抓取今天的資料  
        print(f"\n[時間檢查]: 今天日期 ({DATE_TO_CHECK}) 為交易日。")    
   
        if Now_time_hour > '21':  # 假設在晚上9點後，抓取當天的資料
            print(f"\n[時間檢查]: 現在時間為 {Now_day_time}，抓取 ({TARGET_DATE})當天資料。")
            print("\n" + "="*50)
            print("--- 程式開始執行：TWSE 報告資料抓取 ---")
            print("="*50 + "\n")
        else:
            result_found_pre = check_pre_date_in_csv(Trading_day_file_path, DATE_TO_CHECK)
            TARGET_DATE = result_found_pre[1]  # 抓取上一個交易日的資料
            print(f"\n[時間檢查]: 現在時間為 {Now_day_time}，當天資料尚未更新，將提供前一個交易日 ({TARGET_DATE}) 的資料。")
   
    else:  # 如果今天不是交易日
        result_found_pre = check_pre_date_in_csv(Trading_day_file_path, DATE_TO_CHECK)
        TARGET_DATE = result_found_pre[1]  # 抓取上一個交易日的資料
        print(f"\n[時間檢查]: 今天日期 ({DATE_TO_CHECK}) 為休市日，將提供前一個交易日 ({TARGET_DATE}) 的資料。")
        
    # 設置一個列表來儲存結果(抓取的網路資料)，便於最終預覽
    results = []

    # 轉換TARGET_DATE為YYYYMMDD格式
    TARGET_DATE = TARGET_DATE.replace("/", "")
    # 1. STOCK_DAY (個股日成交資訊)
    # 改以單獨的程式抓取資料
    #results.append(("STOCK_DAY", fetch_twse_stock_day(TARGET_DATE, TARGET_STOCK)))

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
    
    # --- 處理並copy檔案到output資料夾 ---
    input_path = pathlib.Path(__file__).resolve().parent / "datas" / "raw" / "10_TWT44U" / f"{TARGET_DATE}_TWT44U_InvestmentTrust.csv"
    output_dir = pathlib.Path(__file__).resolve().parent / "datas" / "output"
    output_file = f"{TARGET_DATE}_TWT44U_SelectedColumns_Fixed.csv" # 更改檔名以避免覆蓋舊檔案
    select_and_save_columns_fix_encoding(input_path, output_dir, output_file)

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

    time_module.sleep(5) 

    # 增加日誌儲存：記錄本次嘗試抓取的日期
    log_summary_results(results, TARGET_DATE)

    print("\n所有 CSV 檔案已儲存至程式執行目錄下。")
    print("--- 程式執行結束 ---")
    
    # 取得庫存股票清單及近5日收盤價
    # ==========================================================
    # --- 參數設定 ---
    # ==========================================================

    BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    EXCEL_PATH = BASE_DIR + "/datas/股票分析.xlsx"
    SHEET_NAME = "股票庫存統計"
    FILTER_COLUMN = "目前股數庫存統計"
    FILTER_VALUE = "0"
    OUTPUT_DIRECTORY = None 

    # 來源檔案路徑
    SOURCE_FILE = r"Y:\收支記錄\股票分析\股票分析.xlsx"
    # 目標目錄路徑
    DESTINATION_DIR = EXCEL_PATH # 也就是 D 槽的根目錄

    # --- 主要執行區塊 ---

    # 執行複製功能
    copy_file_to_directory(SOURCE_FILE, DESTINATION_DIR)

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

    focused_sheet_name = "關注的股票"
    focused_column_name = "證券名稱"
    focused_stock_names = get_stock_names_from_excel(DESTINATION_DIR, focused_sheet_name, focused_column_name)

    #-- 取得往前6個交易日 ---
    N_DAYS = 6 # 往前找的交易日數量

    recent_trading_days_df = find_last_n_trading_days_with_time_check(Trading_day_file_path, n=N_DAYS)
    #recent_trading_days_df.sort_values(by="日期", ascending=False, inplace=True)
    Send_message_ALL = ""
    for TARGET_STOCK_NAME in TARGET_STOCK_NAMES:
    #    print(f"\n--- {TARGET_STOCK_NAME} 最近 5 個交易日的收盤價 ---")
        Send_message = ""
        #-- 取得五個交易日的收盤價並合併 ---
        CSV_NAME_COLUMN = "證券名稱" # 假設 CSV 中用於名稱比對的欄位
        CSV_PRICE_COLUMN = "收盤價"  # 假設 CSV 中收盤價的欄位

        day_roll = []
        for row in recent_trading_days_df["日期"]:
            TARGET_DATE = row.replace("/", "")
            day_roll.append(TARGET_DATE)

        BASE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

        if recent_trading_days_df is not None:
            print(f"\n--{TARGET_STOCK_NAME}最近5個交易日--")

        CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll[0:1][0]}_BWIBBU_d_IndexReturn.csv"
        get_price_before = lookup_stock_price(
                file_path=CSV_PATH,
                stock_name=TARGET_STOCK_NAME,
                name_col=CSV_NAME_COLUMN,
                price_col=CSV_PRICE_COLUMN
            )
        print("前5交易日收盤價:", get_price_before)
        
        total_price_percent = 0
        for day_roll1 in day_roll[1:]:
            CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll1}_BWIBBU_d_IndexReturn.csv"

            # --- 讀取買賣超資料並發送通知 ---

            file_path = BASE_DIR / "datas" / "raw" / "11_T86" / f"{day_roll1}_T86_InstitutionalTrades.csv"
            stock_name = TARGET_STOCK_NAME # 目標證券名稱

            # 呼叫函式
            net_volume_data = get_stock_net_volume(file_path, stock_name)

            if net_volume_data is not None and not net_volume_data.empty:
                try:
                    # 1. 轉換為數值 (float)，並除以 1000 換算成「張」
                    net_volume_in_lots = net_volume_data.astype(float) / 1000
                    
                    # 2. (可選) 對結果進行四捨五入或取整數
                    # 這裡使用 round() 保持一定精確度，您可以根據需求改為 .astype(int)
                    rounded_lots = net_volume_in_lots.round(0).astype(int) 
                    
                    # 3. 將 Series 轉換為字串 (不含索引，且不含標題)
                    # 使用 to_string(index=False, header=False) 取得純數據字串
                    output_string = rounded_lots.to_string(index=False, header=False).strip()

                except ValueError as e:
                    print(f"❌ 錯誤：數據中包含無法轉換為數值的資料，無法換算成「張」。")
                    # print(f"  詳細錯誤：{e}") # 方便除錯
                  
                     
            else:
                print(f"找不到 {stock_name} 的買賣超股數資料或資料為空。")
                net_volume_data = "0"
            net_volume_data = net_volume_data.tolist()[0][:-4] + "張"
            
            get_price = lookup_stock_price(
                file_path=CSV_PATH,
                stock_name=TARGET_STOCK_NAME,
                name_col=CSV_NAME_COLUMN,
                price_col=CSV_PRICE_COLUMN
            )
            day_mmdd = f"{day_roll1[4:6]}/{day_roll1[-2:]}"
            price_percent = (float(get_price) - float(get_price_before)) / float(get_price_before) * 100
            price_percent = round(float(price_percent), 1)
            
            total_price_percent += int(price_percent)
            
            if price_percent > 0:
                price_percent = f"🔴{abs(price_percent)}"
            else:
                price_percent = f"🟢{abs(price_percent)}"
            
            Send_message += f"{day_mmdd}:{get_price}{price_percent}%({net_volume_data})\n"
            get_price_before = get_price
            
            # 呼叫函式
            stock_indicators_df = get_stock_indicators(CSV_PATH, stock_name)

            pa_ratio = stock_indicators_df.iloc[0]['殖利率(%)']
            pe_ratio = stock_indicators_df.iloc[0]['本益比']
            pb_ratio = stock_indicators_df.iloc[0]['股價淨值比']
            
           # message_add = f"\n--🎯【{stock_name}】個股資訊 🎯--" + f"\n         本益比  : {pe_ratio}%" + f"\n     股價淨值比: {pb_ratio}" + f"\n         殖利率  : {pa_ratio}\n\n"
            message_add = f"\n--🎯【{stock_name}】個股資訊 🎯--\n  本益比  : {pe_ratio}%\n股價淨值比: {pb_ratio}\n  殖利率  : {pa_ratio}\n\n"
            
        if total_price_percent > 0:
            total_price_percent = f"🔴 {abs(total_price_percent)}%"
        else:
            total_price_percent = f"🟢 {abs(total_price_percent)}%"
            
     # 呼叫函式
        top_20_positive_df = get_top_20_institutional_trades_filtered(file_path)
        
        #print(top_20_positive_df)
    
        #sys.exit(1)  # 暫停執行，請確認日期無誤後再移除此行
        # Send_message_ALL += f"\n-{TARGET_STOCK_NAME} 最近5日收盤價-\n{Send_message}\n--三大法人買超前20名--\n{top_20_positive_df}"
        Send_message_ALL += f"發送時間: {Now_day_time}\n"
        Send_message_ALL += f"***************************\n"
        Send_message_ALL += f"📦 {TARGET_DATE} (庫存股)通知📦\n"
        Send_message_ALL += f"***************************\n"
        Send_message_ALL += f"\n=🥇{TARGET_STOCK_NAME} 最近5日收盤價🥇 =\n{Send_message}"
        Send_message_ALL += f"== 近5日績效:{total_price_percent} ==\n"
        Send_message_ALL += message_add  # 加入個股資訊
                
    # 針對關注的股票，取得近5日收盤價
    #Send_focused_message_all = ""
    Send_message_ALL += f"*****************************\n"
    Send_message_ALL += f"💡💡 {TARGET_DATE} 關注股資訊💡💡\n"
    Send_message_ALL += f"*****************************\n"
    for focused_stock_name in focused_stock_names:
    #    print(f"\n--- {focused_stock_names} 最近 5 個交易日的收盤價 ---")
        Send_focused_message = ""
        #-- 取得五個交易日的收盤價並合併 ---
        CSV_NAME_COLUMN = "證券名稱" # 假設 CSV 中用於名稱比對的欄位
        CSV_PRICE_COLUMN = "收盤價"  # 假設 CSV 中收盤價的欄位

        day_roll = []
        for row in recent_trading_days_df["日期"]:
            TARGET_DATE = row.replace("/", "")
            day_roll.append(TARGET_DATE)

        BASE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

        if recent_trading_days_df is not None:
            print(f"\n--{TARGET_STOCK_NAME}最近5個交易日--")

        CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll[0:1][0]}_BWIBBU_d_IndexReturn.csv"
        get_price_before = lookup_stock_price(
                file_path=CSV_PATH,
                stock_name=focused_stock_name,
                name_col=CSV_NAME_COLUMN,
                price_col=CSV_PRICE_COLUMN
            )
        print("前5交易日收盤價:", get_price_before)
        
        total_price_percent = 0
        for day_roll1 in day_roll[1:]:
            CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll1}_BWIBBU_d_IndexReturn.csv"

            # --- 讀取買賣超資料並發送通知 ---

            file_path = BASE_DIR / "datas" / "raw" / "11_T86" / f"{day_roll1}_T86_InstitutionalTrades.csv"
            stock_name = focused_stock_name # 目標證券名稱

            # 呼叫函式
            net_volume_data = get_stock_net_volume(file_path, stock_name)

            if net_volume_data is not None and not net_volume_data.empty:
                try:
                    # 1. 轉換為數值 (float)，並除以 1000 換算成「張」
                    net_volume_in_lots = net_volume_data.astype(float) / 1000
                    
                    # 2. (可選) 對結果進行四捨五入或取整數
                    # 這裡使用 round() 保持一定精確度，您可以根據需求改為 .astype(int)
                    rounded_lots = net_volume_in_lots.round(0).astype(int) 
                    
                    # 3. 將 Series 轉換為字串 (不含索引，且不含標題)
                    # 使用 to_string(index=False, header=False) 取得純數據字串
                    output_string = rounded_lots.to_string(index=False, header=False).strip()

                except ValueError as e:
                    print(f"❌ 錯誤：數據中包含無法轉換為數值的資料，無法換算成「張」。")
                    # print(f"  詳細錯誤：{e}") # 方便除錯
                  
                     
            else:
                print(f"找不到 {stock_name} 的買賣超股數資料或資料為空。")
                net_volume_data = "0"
            net_volume_data = net_volume_data.tolist()[0][:-4] + "張"
            
            get_price = lookup_stock_price(
                file_path=CSV_PATH,
                stock_name=focused_stock_name,
                name_col=CSV_NAME_COLUMN,
                price_col=CSV_PRICE_COLUMN
            )
            day_mmdd = f"{day_roll1[4:6]}/{day_roll1[-2:]}"
            price_percent = (float(get_price) - float(get_price_before)) / float(get_price_before) * 100
            price_percent = round(float(price_percent), 1)
            
            total_price_percent += int(price_percent)
            
            if price_percent > 0:
                price_percent = f"🔴{abs(price_percent)}"
            else:
                price_percent = f"🟢{abs(price_percent)}"
            
            Send_focused_message += f"{day_mmdd}:{get_price}{price_percent}%({net_volume_data})\n"
            get_price_before = get_price
            
        if total_price_percent > 0:
            total_price_percent = f"🔴 {abs(total_price_percent)}%"
        else:
            total_price_percent = f"🟢 {abs(total_price_percent)}%"

        Send_message_ALL += f"\n=⚠️  {focused_stock_name} 最近5日收盤價⚠️  =\n{Send_focused_message}"
        Send_message_ALL += f"== 近5日績效:{total_price_percent} ==\n"
        
    # 將三大法人買超資訊加入
    
    Send_message_ALL += f"\n\n*******************************\n"
    Send_message_ALL += f"🚀{TARGET_DATE}三大法人買超前20名🚀\n"
    Send_message_ALL += f"*******************************\n"
    Send_message_ALL += top_20_positive_df    
    print(Send_message_ALL)
    
    # ---- line notify 發送訊息 ----1
    # ➋ 載入 line_API.env 檔案中的變數
    # 注意：如果您使用 .env 以外的檔名 (如 line_token.env)，需要指定檔名

    LINE_API_ENV_PATH = BASE_DIR / "line_API.env"
    load_dotenv(LINE_API_ENV_PATH)

    # ➌ 從環境變數中讀取 Token 和 User ID
    LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    LINE_USER_ID = os.getenv("LINE_USER_ID")


    # 修正 LineBotApiError 的匯入路徑（根據您上一個問題的解答）
    from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
    from linebot.v3.messaging import TextMessage, PushMessageRequest


    # ----------------- 檢查 Token 是否存在 -----------------
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
        exit()
    # ----------------------------------------------------

    try:
        # 初始化 Configuration 和 MessagingApi
        configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
        api_client = ApiClient(configuration)
        messaging_api = MessagingApi(api_client)
        print("Line Bot API 初始化成功。")
    except Exception as e:
        print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")

    # ... 接下來的程式碼保持不變 ...
    # 這是接收訊息的用戶 ID 或群組 ID
    # LINE_USER_ID 現在已經從 .env 檔案中讀取

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


    # 發送全部資訊(庫存股通知、關注股通知、三大法人買超前20)
    analysis_report = Send_message_ALL
    #send_stock_notification(LINE_USER_ID, analysis_report)
# ===========================================================

# 1. 初始化運行狀態
running = True

# 先運行 schedule.clear() 將排程清除，避免習慣使用 jupyter notebook 整合開發環境的讀者，
# 有殘存的排程，造成運行結果不如預期
schedule.clear()

# 指定每 15 秒運行一次 say_hi 函數
# schedule.every(200).seconds.do(main_run)
# print("✅ 已設定定時任務：每1秒執行 main_run。")
#每小時運行一次
# schedule.every(1).hour.do(main_run)
# print("✅ 已設定定時任務：每小時執行 main_run。")

# 每天 15:30 運行一次 get_price 函數
schedule.every().day.at('21:00').do(main_run)
print("✅ 已設定定時任務：21:00 執行 main_run。")

# 3. 設定鍵盤熱鍵 (非阻塞式監聽)
keyboard.add_hotkey('1', main_run)
keyboard.add_hotkey('q', stop_program)
print("✅ 已設定鍵盤熱鍵：[1] 執行main_run, [Q] 停止程式。")

print("\n--- 程式開始運行 ---")
print("主程式和排程監聽中...")

# --- 主循環 (Main Loop) ---
try:
    while running:
        # 1. 檢查是否有排程任務需要運行
        schedule.run_pending()
        
        # 2. 讓主循環短暫休眠，同時讓 CPU 資源釋放給其他行程 (包括鍵盤監聽)
        # 這裡設定一個較短的休眠時間，確保對排程和鍵盤輸入的響應更即時。
        time_module.sleep(1)
        
except KeyboardInterrupt:
    # 允許使用 Ctrl+C 退出
    print("\n程式被 Ctrl+C 中斷退出。")

finally:
# 3. 移除所有註冊的熱鍵 (清理環境)
    keyboard.unhook_all()
    print("所有鍵盤監聽已關閉。")
    print("程式安全退出。")