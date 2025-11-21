# main_script.py 內容
import subprocess
import sys

# 執行的命令： 'python' (解釋器) + 'other_script.py' (目標檔案) + 'Hello' (傳入參數)
command = [sys.executable, r'D:\Python_repo\python\Jason_Stock_Analyzer\1121-單獨抓取資料-3_BWIBBU_d完成.py', 'Hello']

try:
    # 使用 subprocess.run() 執行命令並等待其完成
    # capture_output=True 會捕獲標準輸出和標準錯誤
    # text=True 將輸出解碼為字串
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    
    print("--- Subprocess 執行結果 ---")
    print(f"標準輸出 (stdout):\n{result.stdout}")
    print(f"返回碼 (returncode): {result.returncode}")
    
except subprocess.CalledProcessError as e:
    # 如果 check=True 且返回碼非零，則會拋出此錯誤
    print(f"執行失敗，錯誤訊息 (stderr):\n{e.stderr}")
    print(f"返回碼 (returncode): {e.returncode}")
    
except FileNotFoundError:
    print("錯誤：找不到 Python 解釋器或 other_script.py 檔案。")