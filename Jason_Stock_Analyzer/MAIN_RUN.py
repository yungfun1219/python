import subprocess
import sys
import os

# 1. 定義要執行的腳本路徑
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
filename = BASE_DIR + "/get_trading_day.py"

#filename = r'D:\your_project\analysis_script.py' 

# 2. 檢查檔案是否存在
if not os.path.exists(filename):
    print(f"錯誤：找不到腳本 {filename}")
else:
    print(f"--- 準備以子程序執行腳本：{filename} ---")
    
    try:
        # 使用 sys.executable 確保使用當前的 Python 直譯器
        result = subprocess.run(
            [sys.executable, filename],
            capture_output=True,  # 捕獲腳本的輸出
            text=True,            # 以文本模式處理輸出
            encoding='utf-8',    # <--- 新增此行，強制使用 UTF-8 處理輸出
            errors='ignore',     # <--- (可選) 遇到無法解碼的位元組時，直接忽略而不是拋出錯誤
            check=True            # 如果腳本返回非零錯誤碼，則拋出異常
        )
        
        print(f"✅ 腳本執行完成 (回傳碼: {result.returncode})。")
        print("\n--- 腳本輸出內容 (Stdout) ---")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ 腳本執行失敗，錯誤碼: {e.returncode}")
        print("\n--- 腳本錯誤輸出內容 (Stderr) ---")
        print(e.stderr)
    except Exception as e:
        print(f"發生執行時錯誤: {e}")