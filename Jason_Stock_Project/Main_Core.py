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
from typing import Optional, Tuple, List, Union, Dict, Any
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
    col_space_width = 10 

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
        
        top_20_trades += f"{index_no_str}." + f"{new_current_volume_column}" + f" ({rol[volume_col_display_name]}張)⚪️🔴\n"
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

# 抓取指定日期的 T86 報告 (三大法人買賣超彙總表 - 依類別)。
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
        trading_days_found.reverse()

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

# 讀取、清理單一 CSV 檔案，並篩選出三大法人買超股數最大的 Top N 股票。
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
        
        # 篩選條件：1. 代號為 4 位數字 | 2. 買賣超股數 > 1000 股 (買超)
        df_filtered = df[
            (df[code_column].str.match(r'^\d{4}$')) & 
            (df[volume_column] > 1000) 
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

# 分析最新一日三大法人買超 Top N 股票在過去 N 天的回溯趨勢
def analyze_top_stocks_trend(
    file_paths: List[str],
    top_n: int = 30, 
    n_days_lookback: int = 5, 
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號"
) -> Optional[str]:
    """
    分析最新一日三大法人買超 Top N 股票在過去 N 天的回溯趨勢。
    輸出結果不包含名次，並依 代號, 證券名稱, 回溯趨勢, 買超張數 排序。
    回溯趨勢的日期標籤和標記皆以「最舊日到最新日」的順序排列。

    Args:
        file_paths (List[str]): 依序為 [最新日, 前一日, ..., 前第 N 日] 的檔案路徑列表。
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
    df_base_day: Optional[pd.DataFrame] = None 

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
        
        # 基準日 (最新日) 的數據需要單獨保存
        if i == 0:
            df_base_day = df_day
    
    if df_base_day is None or df_base_day.empty:
        print("❌ 基準日 (最新日) 數據無效或為空，無法進行趨勢分析。")
        return None
        
    # --- 2. 獲取基準日 Top N 股票代號 (此為分析的主體) ---
    base_stocks = df_base_day[['代號', '名稱', '股數']].head(top_n).copy()
    
    # --- 3. 執行回溯趨勢分析 ---
    
    # 建立列表用於存放每天的回溯標記 (Series) 和日期標籤
    # 初始順序為 i=1 到 n_days_lookback (最新回溯日到最舊回溯日)
    trend_header_parts: List[str] = []
    trend_marker_series: List[pd.Series] = []
    
    for i in range(1, n_days_lookback + 1):
        # 回溯日期的標籤
        day_tag = day_labels[i].replace("/", "")[-4:] # 取日期後四碼 (如 1110)
        trend_header_parts.append(day_tag)
        
        # 取得該回溯日期的 Top N 股票代號集合
        past_top_n_codes = all_day_data.get(i, set())
        
        # 對基準日的每一檔股票進行檢查
        presence_markers = []
        for code in base_stocks['代號']:
            # 檢查代號是否出現在過去 Top N 列表中
            if code in past_top_n_codes:
                presence_markers.append("🔴") # 存在 (多加一個空格以確保間隔一致)
            else:
                presence_markers.append("⚪️") # 不存在
        
        # 將標記 Series 存入列表
        trend_marker_series.append(pd.Series(presence_markers, index=base_stocks.index))
        
    # *** 關鍵修改：將列表反轉，使順序變為「最舊日到最新日」 ***
    trend_header_parts.reverse()
    trend_marker_series.reverse()
    
    # 建立最終的回溯趨勢標頭 (最舊日到最新日)
    trend_header = " ".join(trend_header_parts)
    
    # 串接所有的趨勢標記 Series (最舊日到最新日)
    if trend_marker_series:
        # 使用 copy() 確保獨立性
        base_stocks['趨勢'] = trend_marker_series[0].copy() 
        for series in trend_marker_series[1:]:
            base_stocks['趨勢'] += series
    else:
        base_stocks['趨勢'] = pd.Series("", index=base_stocks.index) # 避免空列表錯誤

    # 移除趨勢欄位尾部多餘的空格
    base_stocks['趨勢'] = base_stocks['趨勢'].str.strip()
    
    # --- 4. 格式化輸出結果 ---
    
    # 將股數轉換為張數並格式化
    volume_col_display_name = '買超張數'
    base_stocks[volume_col_display_name] = base_stocks['股數'].apply(lambda x: f"{int(x / 1000):,}")
    
    # 總寬度調整 (配合新順序與欄位)
    TOTAL_WIDTH = 28
    
    # 建立表格標頭 - 移除名次，調整順序: 代號 | 證券名稱 | 回溯趨勢 | 買超張數
    
    output_lines = [
        f"\n*******************************"
        f"\n   📈 三大法人買超Top{top_n}\n基準日:{day_labels[0]}-過去{n_days_lookback}日趨勢"
        f"\n*******************************",
    #    f"{'代號'.center(6)} | {'證券名稱'.center(6)} | 回溯趨勢 > > > > {day_labels[0][-4:]}  | {'買超張數'.center(8)}", 
    #    "-" * TOTAL_WIDTH
    ]
    
    # 建立表格內容
    for index, row in base_stocks.iterrows():
        #code_str = str(row['代號']).center(6)
        name_str = row['名稱'].ljust(4, '　') # 中文佔用寬度處理
        volume_str = row[volume_col_display_name].rjust(6) # 右對齊數字
        trend_str = row['趨勢'] 

        show_width_in_stock_name = 4
        if len(name_str.replace(' ', '')) < show_width_in_stock_name :
            padding_width = show_width_in_stock_name - len(name_str.replace(' ', ''))
            name_str = name_str.replace(' ', '') + '  ' * padding_width
        else:
            name_str = name_str.replace(' ', '')

        # 輸出順序: 代號 | 證券名稱 | 回溯趨勢 | 買超張數
        #output_lines.append(f"{code_str} | {name_str} | {trend_str} | {volume_str}")
        output_lines.append(f"{name_str}{trend_str}({volume_str}張)")
        
        #print(f"✅ {name_str.replace('  ', '')}")
        #sys.exit(1)  # 暫停執行，請確認日期無誤後再移除此行
    output_lines.append("=" * TOTAL_WIDTH)
    output_lines.append(f"🔴: 該日出現在 Top {top_n} 名單中 \n⚪️: 該日未出現在 Top {top_n} 名單中")
    
    return "\n".join(output_lines)

# 抓取網路資料 "get_1-11-上市股票.py"
# ==========================================================
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
            time_module.sleep(delay_seconds)
        else:
            print(f"❌ {target_date} 資料經過 {max_attempts} 次嘗試後仍然失敗。")
            break
    
    # 3. 只有在進行了網路抓取或重試之後，才需要等待 2 秒
    if is_successful or attempt == max_attempts:
        time_module.sleep(2)

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
                time_module.sleep(delay_seconds)
            else:
                print(f"❌ {stock_no} | {month_str} 資料經過 {max_attempts} 次嘗試後仍然失敗，跳過此股票。")
                tasks_failed += 1
                break
                
        # 3. 每次嘗試網路請求後，等待 2 秒 (無論成功或失敗)
        if is_successful or attempt == max_attempts:
            time_module.sleep(2)
            
    print(f"\n--- 🏁 STOCK_DAY 抓取結束。成功覆蓋: {tasks_successful}, 失敗: {tasks_failed} ---")
# ==========================================================

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
# 設定您想要抓取的目標日期 (只需修改此處即可抓取所有報告的資料)
def main_run():
    #----------------
    global running # 引用全局變數
    
    if not running:
        # 如果在等待執行的排程隊列中，檢查到不運行，則直接跳過
        print("\n[定時任務]: 偵測到退出信號，跳過本次執行。")
        return

    # --- 區塊 1: 日期與交易日判斷 (優化日期格式化) ---
    NOW_DATETIME = datetime.now()
    Now_day_time = NOW_DATETIME.strftime("%Y-%m-%d %H:%M")  #取得目前系統時間的日期及時間「例如 2025-11-12 11:12」
    Now_time_year = NOW_DATETIME.strftime("%Y")  #取得目前系統時間的「年」
    Trading_day_file_path = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / f"trading_day_2021-{Now_time_year}.csv"
    
    # 決定當前要抓取的目標日期 (TARGET_DATE)    
    TARGET_DATE = NOW_DATETIME.strftime("%Y%m%d") 
    DATE_TO_CHECK = NOW_DATETIME.strftime("%Y/%m/%d")  
    DATE_TO_CHECK_NOW = NOW_DATETIME.strftime("%Y/%m/%d %H:%M:%S")
    
    # 處理要抓取哪一天的資料邏輯
    result_found_days = get_previous_n_trading_days(Trading_day_file_path, DATE_TO_CHECK_NOW)
    
    if result_found_days == None:
        DATE_TO_CHECK_NOW = DATE_TO_CHECK_NOW - timedelta(days=1)    
        result_found_days = get_previous_n_trading_days(Trading_day_file_path, DATE_TO_CHECK_NOW)
    
    if DATE_TO_CHECK == result_found_days[-1]:  # 如果今天是交易日
        TARGET_DATE = DATE_TO_CHECK  # 抓取今天的資料  
        print(f"\n[時間檢查]: 今天日期 ({DATE_TO_CHECK}) 為交易日。")    
        # 抓取當天的資料
        print(f"\n[時間檢查]: 現在時間為 {DATE_TO_CHECK_NOW}，抓取 ({TARGET_DATE})當天資料。")
        print("\n" + "="*50)
        print("--- 程式開始執行：TWSE 報告資料抓取 ---")
        print("="*50 + "\n")
    else:
        TARGET_DATE = result_found_days[-1]  # 抓取前一天的資料  
        print(f"\n[時間檢查]: 現在時間為 {DATE_TO_CHECK_NOW}，當天資料尚未更新，將提供前一個交易日 ({TARGET_DATE}) 的資料。")
        print("\n" + "="*50)
        print("--- 程式開始執行：TWSE 報告資料抓取 ---")
        print("="*50 + "\n")

    time_module.sleep(2) 
    
    # 抓取網路資料 "get_1-11-上市股票.py"
    # ==========================================================
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
        print("測試OK。")
    elif not stock_list:
        print("警告：無法取得股票清單 (stocks_all.csv)，跳過 STOCK_DAY 抓取。")
    elif not monthly_date:
        print("警告：無法取得目標月份，跳過 STOCK_DAY 抓取。")
        
    print("\n======================================")
    print("✅ 所有 TWSE 數據抓取任務已完成。")
    print(f"【開始時間】{start_time} ")
    print(f"【完成時間】{datetime.now().strftime('%H:%M:%S')} ")
    print("======================================")
    # ==========================================================

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
            
    Send_message_ALL = ""
    Send_message_ALL += f"發送時間: {Now_day_time}\n"
    Send_message_ALL += f"***************************\n"
    Send_message_ALL += f"📦 {DATE_TO_CHECK} (庫存股)通知📦\n"        
    Send_message_ALL += f"***************************"
    for TARGET_STOCK_NAME in TARGET_STOCK_NAMES:
        Send_message = ""
        #-- 取得五個交易日的收盤價並合併 ---
        CSV_NAME_COLUMN = "證券名稱" 
        CSV_PRICE_COLUMN = "收盤價" 

        day_roll = []
        for row in recent_trading_days_df["日期"]:
            TARGET_DATE = row.replace("/", "")
            day_roll.append(TARGET_DATE)

        BASE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

        if recent_trading_days_df is not None:
            print(f"\n--{TARGET_STOCK_NAME}最近5個交易日--")

        # 獲取第 5 個交易日 (day_roll[0]) 的收盤價作為比較基準
        CSV_PATH_BEFORE = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll[0]}_BWIBBU_d_IndexReturn.csv"
        get_price_before = lookup_stock_price(
            file_path=CSV_PATH_BEFORE,
            stock_name=TARGET_STOCK_NAME,
            name_col=CSV_NAME_COLUMN,
            price_col=CSV_PRICE_COLUMN
        )
        print("前5交易日收盤價:", get_price_before)
        
        total_price_percent = 0
        final_indicators = {} # 用於儲存最後一天的個股指標

        # 迴圈處理最近 4 個交易日的數據 (day_roll[1] 到 day_roll[4])
        for day_roll1 in day_roll[1:]:
            
            # 呼叫新的函式來獲取當日所有數據
            day_data = get_day_stock_details(
                day_roll1=day_roll1,
                target_stock_name=TARGET_STOCK_NAME,
                base_dir=BASE_DIR,
                get_price_before=get_price_before,
                csv_name_column=CSV_NAME_COLUMN,
                csv_price_column=CSV_PRICE_COLUMN
            )
            
            # 彙總績效
            total_price_percent += day_data['price_percent'] # price_percent 是數字
            #day_data_net_volume_data = day_data['net_volume_data']
            # 建立單日訊息
            Send_message += (
                f"{day_data['day_mmdd']}:"
                f"{day_data['get_price']}"
                f"{day_data['price_percent_formatted']}"
                #f"({day_data_net_volume_data}張)\n"
                f"({day_data['net_volume_data']})\n"
            )
            
            # 更新前一日價格，用於下一輪迭代
            get_price_before = day_data['get_price']
            
            # 儲存最後一天的指標，用於報表尾部 (假設您只需要最後一天的指標)
            final_indicators = {
                'pa_ratio': day_data['pa_ratio'],
                'pe_ratio': day_data['pe_ratio'],
                'pb_ratio': day_data['pb_ratio'],
            }

        # ------------------ 報表尾部處理 (使用儲存的 final_indicators) ------------------

        # 處理總體漲跌幅
        total_price_percent = round(total_price_percent, 1) # 確保總計也是四捨五入
        if total_price_percent > 0:
            total_price_percent_formatted = f"🔴 {abs(total_price_percent)}%"
        else:
            total_price_percent_formatted = f"🟢 {abs(total_price_percent)}%"
        
        # 建立個股資訊訊息
        message_add = (
            f"\n--🎯【{TARGET_STOCK_NAME}】個股資訊 🎯--\n"
            f"  本益比  : {final_indicators['pe_ratio']}\n"
            f"股價淨值比: {final_indicators['pb_ratio']}\n"
            f"  殖利率  : {final_indicators['pa_ratio']}%\n\n"
        )

        # 彙總總體訊息
        # 假設 TARGET_DATE 是 day_roll[0] (最近一天) 的日期，請確保這裡的 TARGET_DATE 是正確的
        TARGET_DATE = f"{day_roll[1][4:6]}/{day_roll[1][-2:]}" # 暫時使用 day_roll[1] 的日期作為報表日期
        

        Send_message_ALL += f"\n=🥇{TARGET_STOCK_NAME} 最近5日收盤價🥇 =\n{Send_message}"
        Send_message_ALL += f"== 近5日績效:{total_price_percent_formatted} ==\n"
        Send_message_ALL += message_add # 加入個股資訊

        ##2025-11-20
                
    # 針對關注的股票，取得近5日收盤價
    #Send_focused_message_all = ""
    Send_message_ALL += f"*****************************\n"
    Send_message_ALL += f"💡 {DATE_TO_CHECK} 關注股資訊💡\n"
    Send_message_ALL += f"*****************************"
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
            net_volume_series = get_stock_net_volume(file_path, stock_name) # 變數名稱改為 net_volume_series 以示區別
            net_volume_data = "0張" # 預設值

            if net_volume_series is not None and not net_volume_series.empty:
                try:
                    # 1. 字串清理：處理潛在的逗號或負號，並強制轉換為 float
                    cleaned_str = net_volume_series.astype(str).str.replace(',', '', regex=False).str.replace('-', '', regex=False).str.strip()
                    # 使用 pd.to_numeric 進行穩健轉換
                    net_volume_in_lots = pd.to_numeric(cleaned_str, errors='coerce').iloc[0] / 1000
                    
                    # 2. 四捨五入並格式化
                    if pd.notna(net_volume_in_lots):
                        # 格式化為帶有千位分隔符的整數字串，並加上 "張"
                        net_volume_data = f"{int(round(net_volume_in_lots, 0)):,d}張"
                    else:
                        net_volume_data = "資料錯誤"

                except Exception as e:
                    print(f"❌ 錯誤：{focused_stock_name} 數據轉換失敗，跳過買賣超換算 ({e})。")
                    net_volume_data = "資料錯誤" 
            else:
                print(f"找不到 {focused_stock_name} 的買賣超股數資料或資料為空。")
                net_volume_data = "0張" # 保持預設值

            # ⚠️ 原始程式碼的報錯行已移除！
            # net_volume_data = net_volume_data.tolist()[0][:-4] + "張"
            CSV_PATH = BASE_DIR / "datas" / "raw" / "3_BWIBBU_d" / f"{day_roll1}_BWIBBU_d_IndexReturn.csv"
            get_price = lookup_stock_price(
                file_path=CSV_PATH,
                stock_name=focused_stock_name,
                name_col=CSV_NAME_COLUMN,
                price_col=CSV_PRICE_COLUMN
            )
            day_mmdd = f"{day_roll1[4:6]}/{day_roll1[-2:]}"
            
            print("測試:",CSV_PATH)
            print("測試:",focused_stock_name)
            print("測試:",CSV_NAME_COLUMN)
            print("測試:",CSV_PRICE_COLUMN)
            print("測試:",get_price)
            
            #sys.exit(1)  # 暫停執行，請確認日期無誤後再移除此行
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
    
    Send_message_ALL += f"\n\n"
    #Send_message_ALL += analysis_result    
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
    send_stock_notification(LINE_USER_ID, analysis_report)
# ===========================================================

# 1. 初始化運行狀態
running = True

# 先運行 schedule.clear() 將排程清除，避免習慣使用 jupyter notebook 整合開發環境的讀者，
schedule.clear()

# 指定每 15 秒運行一次 say_hi 函數
# schedule.every(200).seconds.do(main_run)

#每小時運行一次
# schedule.every(1).hour.do(main_run)
# print("✅ 已設定定時任務：每小時執行 main_run。")

# 每天 21:00 執行一次
schedule.every().day.at('21:00').do(main_run)
print("✅ 已設定定時任務：21:00 執行 main_run。")

# 3. 設定鍵盤熱鍵 (非阻塞式監聽)
keyboard.add_hotkey('1', main_run)
keyboard.add_hotkey('q', stop_program)
print("✅ 已設定鍵盤熱鍵：[1] 直接執行main_run, [Q] 停止程式。")

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