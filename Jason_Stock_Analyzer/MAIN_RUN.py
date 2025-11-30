import subprocess
import sys
import os
import io

# 這是解決 Windows 環境下 cp950 無法顯示 Emoji 的標準方式
# 強制設定標準輸出和錯誤輸出的編碼為 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
# 後續所有 print() 輸出都會使用 UTF-8

# 1. 定義要執行的腳本路徑
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 確保路徑分隔符號在不同系統上是正確的
# 【修正點】: 使用 os.path.join 構建路徑，確保路徑的兼容性
script_name = "取得庫存股票.py"
filename = os.path.join(BASE_DIR, script_name)

# 2. 檢查檔案是否存在
if not os.path.exists(filename):
    # 嘗試用 BASE_DIR + script_name 的方式列印，以防路徑本身就是亂碼
    print(f"錯誤：找不到腳本 {filename}")
else:
    # 由於 print 的 sys.stdout 已經設定為 UTF-8，這裡可以安全地使用 Emoji
    print(f"--- 準備以子程序執行腳本：{filename} ---")
    
    try:
        # 使用 sys.executable 確保使用當前的 Python 直譯器
        result = subprocess.run(
            [sys.executable, filename],
            capture_output=True,  # 捕獲腳本的輸出
            text=True,            # 以文本模式處理輸出
            encoding='utf-8',     # <--- 確保子程序輸出以 UTF-8 解碼
            errors='ignore',      # <--- 遇到無法解碼的位元組時，忽略
            check=True            # 如果腳本返回非零錯誤碼，則拋出異常
        )
        
        print(f"✅ 腳本執行完成 (回傳碼: {result.returncode})。")
        print("\n--- 腳本輸出內容 (Stdout) ---")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        # 捕捉子程序錯誤，並將其 stdout/stderr 輸出
        print(f"❌ 腳本執行失敗，錯誤碼: {e.returncode}")
        print("\n--- 腳本錯誤輸出內容 (Stderr) ---")
        # 由於 subprocess 捕獲的輸出已經過 encoding='utf-8' 解碼，這裡可以直接安全地 print
        # 如果子程序內部仍有未經處理的 cp950 輸出，errors='ignore' 會幫忙緩解
        print(e.stderr)
        
    except Exception as e:
        print(f"發生執行時錯誤: {e}")