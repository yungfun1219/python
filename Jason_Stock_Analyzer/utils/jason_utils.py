from pathlib import Path
import os
import pandas as pd
from typing import Tuple # 使用 typing.Tuple 來更好地註解函式回傳類型
import re
from typing import Optional

# 民國年轉西元年
def transform_date(date):
    try:
        if '/' in date:
            Y, m, d = date.split('/')
        elif '-' in date:
            Y, m, d = date.split('-')
        else:
            raise ValueError("日期格式錯誤，無法辨識分隔符號")
    except Exception as e:
        print(f"轉換日期時發生錯誤: {e}")
        return date  # 發生錯誤時，回傳原始日期
    
    return str(int(Y) + 1911) + '/' + m + '/' + d

# 輸入檔案路徑字串，回傳 (資料夾完整路徑, 檔案名稱)。

def get_path_to_folder_file(file_path_str: str) -> tuple[str, str]:
    """
    使用 pathlib 模組。
    參數:
        file_path_str (str): 檔案的完整路徑字串。
    回傳:
        tuple[str, str]: (directory_path, file_name)
    """
    # 1. 建立 Path 物件
    p = Path(file_path_str)
    
    # 2. 獲取資料夾路徑 (Path.parent 會自動處理路徑分隔符)
    directory_path = str(p.parent)
    
    # 3. 獲取檔案名稱
    file_name = p.name
    
    return directory_path, file_name

# 檢查並建立資料夾。如果資料夾已存在則不做任何動作。
def check_and_create_folder(folder_path: str | Path) -> bool:
    """
    參數:
        folder_path (str | Path): 要檢查和/或建立的資料夾路徑。
    回傳:
        bool: 成功建立或資料夾已存在時回傳 True；否則回傳 False。
    """
    path = Path(folder_path)
    
    if path.is_dir():
        # 情況 1: 資料夾已存在
        # print(f"資料夾 '{path}' 已存在。")
        return True
    try:
        # 情況 2: 資料夾不存在，使用 exist_ok=True 遞迴建立
        path.mkdir(parents=True, exist_ok=True)
        print(f"資料夾 '{path}' 成功建立。")
        return True
    except PermissionError:
        print(f"錯誤：沒有權限建立資料夾 '{path}'。")
        return False
    except FileExistsError:
        # 僅當路徑存在但它是一個檔案時會拋出此錯誤
        print(f"錯誤：路徑 '{path}' 存在，但它是一個檔案，不是資料夾。")
        return False
    except Exception as e:
        print(f"無法建立資料夾 '{path}'，發生未知錯誤: {e}")
        return False

# 檢查檔案是否存在且確實是一個檔案 (非資料夾)。
def check_file_exists(file_path: str | Path) -> bool:
    """
    參數:
        file_path (str | Path): 要檢查的檔案路徑。
    回傳:
        bool: 檔案存在時回傳 True；否則回傳 False。
    """
    path = Path(file_path)
    
    if path.is_file():
        # print(f"檔案 '{path}' 存在。")
        return True
    elif path.exists():
        # 如果路徑存在但不是檔案 (例如是資料夾)
        print(f"錯誤：路徑 '{path}' 存在，但它不是一個檔案。")
        return False
    else:
        # 檔案不存在
        print(f"錯誤：檔案不存在於 '{path}'。")
        return False
    
# 從指定的日誌檔案中讀取內容，並使用正規表達式擷取指定前綴 (target_prefix)
def get_extracted_string(file_path: str, target_prefix: str) -> Optional[str]:
    """
    後面的數字字串。
    Args:
        file_path (str): 日誌檔案 (例如 get_company_all.log) 的完整路徑。
        target_prefix (str): 要尋找的前綴字串 (例如 "最終總筆數:")。
    Returns:
        Optional[str]: 如果成功擷取到數字字串，則回傳該字串；
                      如果檔案不存在或找不到匹配的數字，則回傳 None。
    """
    # 1. 檢查檔案是否存在
    if not os.path.exists(file_path):
        print(f"錯誤：檔案不存在於 {file_path}")
        return None

    try:
        # 2. 開啟並讀取檔案內容 (使用 'r' 模式讀取，並使用 with open 確保檔案會自動關閉)
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 3. 構造正規表達式
        # re.escape() 確保目標前綴中的特殊字元被正確處理
        # \s* 匹配零個或多個空白字元
        # (\d+) 匹配一個或多個數字，並將其作為一個群組 (Group) 捕獲
        # 使用傳入的 target_prefix 變數
        pattern = re.escape(target_prefix) + r"\s*(\d+)"
        
        # 4. 搜尋匹配項
        match = re.search(pattern, content)

        if match:
            # 5. 提取匹配到的數字群組 (Group 1)，回傳字串
            extracted_string = match.group(1)
            print(f"成功擷取到 '{target_prefix}' 後面的字串為: {extracted_string}")
            return extracted_string
        else:
            print(f"錯誤：在檔案中找不到符合 '{target_prefix}' 模式的數字。")
            return None

    except Exception as e:
        print(f"讀取或處理檔案時發生錯誤: {e}")
        return None
