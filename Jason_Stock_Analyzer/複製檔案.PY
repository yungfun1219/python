import shutil
import os
from pathlib import Path

# 來源檔案路徑
SOURCE_FILE = r"Y:\收支記錄\股票分析\股票分析.xlsx"
# 目標目錄路徑
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
EXCEL_PATH = BASE_DIR + "/datas/股票分析.xlsx"
DESTINATION_DIR = EXCEL_PATH # 也就是 D 槽的根目錄

# 將指定檔案複製到目標目錄。
def copy_file_to_directory(source: str, destination: str):
    """
    將指定檔案複製到目標目錄。
    """
    source_path = Path(source)
    destination_dir_path = Path(destination)
    
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

# 執行複製功能
copy_file_to_directory(SOURCE_FILE, DESTINATION_DIR)