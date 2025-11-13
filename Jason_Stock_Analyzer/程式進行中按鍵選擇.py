import schedule
import time
import keyboard
import sys # 匯入 sys 以便在需要時強制退出

# 1. 初始化運行狀態 (確保是全局變數)
running = True

# --- 任務定義 ---
# 

def process_one():
    """按鍵 [1] 觸發的額外流程"""
    print("\n\n--- 流程 1 (手動觸發) 啟動：執行緊急數據報表 ---")
    # 執行您需要的額外程式流程
    time.sleep(2) 
    print("--- 流程 1 結束：報表已生成 ---")

def process_two():
    """按鍵 [2] 觸發的額外流程"""
    print("\n\n--- 流程 2 (手動觸發) 啟動：強制重新連線資料庫 ---")
    # 執行您需要的額外程式流程
    time.sleep(1)
    print("--- 流程 2 結束：資料庫連線已重置 ---")

def stop_program():
    """按鍵 [Q] 停止程式"""
    print("\n\n👋 偵測到 'Q' 鍵，程式即將安全退出...")
    global running
    running = False


def main_run():
    """定時排程的任務 (每小時執行一次)"""
    global running # 引用全局變數

    if not running:
        # 如果在等待執行的排程隊列中，檢查到不運行，則直接跳過
        print("\n[定時任務]: 偵測到退出信號，跳過本次執行。")
        return

    print("\n[定時任務]: main_run 正在執行...")
    
def stop_program():
    """按鍵 [Q] 停止程式"""
    print("\n\n👋 偵測到 'Q' 鍵，程式即將安全退出...")
    global running
    running = False
    
    # 額外措施：在某些情況下，如果主循環被完全鎖死，
    # 您可能需要在這裡調用 sys.exit()。
    # 但由於我們加入了 `finally` 塊進行清理，通常不建議這樣做，除非是最終手段。
    # sys.exit(0) 

# ... (其他程式碼與設定保持不變) ...
# 1. 初始化運行狀態
running = True

# 2. 設定定時排程
schedule.every(1).hour.do(main_run)
print("✅ 已設定定時任務：每小時執行 main_run。")

# 3. 設定鍵盤熱鍵 (非阻塞式監聽)
keyboard.add_hotkey('1', process_one)
keyboard.add_hotkey('2', process_two)
keyboard.add_hotkey('q', stop_program)
print("✅ 已設定鍵盤熱鍵：[1] 觸發流程 1, [2] 觸發流程 2, [Q] 停止程式。")

print("\n--- 程式開始運行 ---")
print("主程式和排程監聽中...")


# --- 主循環 (Main Loop) ---
try:
    while running:
        # 1. 檢查是否有排程任務需要運行
        schedule.run_pending()
        
        # 2. 讓主循環短暫休眠，同時讓 CPU 資源釋放給其他行程 (包括鍵盤監聽)
        # 這裡設定一個較短的休眠時間，確保對排程和鍵盤輸入的響應更即時。
        time.sleep(1)
        
except KeyboardInterrupt:
    # 允許使用 Ctrl+C 退出
    print("\n程式被 Ctrl+C 中斷退出。")

finally:
# 3. 移除所有註冊的熱鍵 (清理環境)
    keyboard.unhook_all()
    print("所有鍵盤監聽已關閉。")
    print("程式安全退出。")