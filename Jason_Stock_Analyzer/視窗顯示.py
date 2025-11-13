import tkinter as tk
from tkinter import scrolledtext
import sys
import time
import threading

# 1. 輸出重新導向器 (Redirection Class)
class TextRedirector:
    """
    將 stdout (print 輸出) 重新導向到 Tkinter Text Widget 的類別。
    """
    def __init__(self, widget):
        self.widget = widget
        self.widget.tag_configure("stdout", foreground='black') # 標準輸出顏色
        self.widget.tag_configure("error", foreground='red')    # 錯誤輸出顏色

    def write(self, string):
        # 插入文字到 Text Widget 末尾
        self.widget.insert(tk.END, string, "stdout")
        # 自動滾動到底部
        self.widget.see(tk.END)
        # 確保 GUI 立即更新 (在多線程環境下，這是確保更新的關鍵)
        self.widget.update_idletasks()

    def flush(self):
        # Tkinter 不需 flush
        pass

# 2. 模擬您的程式邏輯 (在獨立線程中運行)
def main_program_logic(start_button, stop_button, stop_event):
    """
    這個函式模擬您原本在主程式中執行的邏輯，並使用 print() 輸出。
    
    Args:
        start_button: Tkinter Button 物件，用於在程式結束時重新啟用。
        stop_button: Tkinter Button 物件，用於在程式執行時啟用。
        stop_event: threading.Event 物件，用於接收停止信號。
    """
    print("\n--- 模擬程式開始執行 ---")
    
    was_stopped = False
    for i in range(1, 100): # 增加循環次數，讓停止效果更明顯
        # 讓線程休眠 1 秒，但同時檢查是否有停止信號
        # 如果 stop_event.wait(1) 在 1 秒內被 set()，它會立即返回 True
        if stop_event.wait(0.5): 
            was_stopped = True
            break
        
        print(f"[步驟 {i}] 正在處理數據...")
        
        if i % 10 == 0:
            print(">>> 數據檢查點達成，繼續運作...")
    
    if was_stopped:
        print("\n--- 程式被使用者提前中止 ---")
    else:
        print("\n--- 模擬程式執行完畢 ---")
    
    # 程式執行完畢或中止後，更新 GUI 狀態 (在主線程中執行)
    # 使用 root.after 確保線程安全地更新 GUI 元件
    root = start_button.winfo_toplevel()
    root.after(0, lambda: start_button.config(state=tk.NORMAL, text="點擊開始執行程式"))
    root.after(0, lambda: stop_button.config(state=tk.DISABLED))


# 3. 核心 GUI 建立函式
def run_gui_application():
    """
    建立並運行帶有輸出顯示器的 Tkinter GUI 應用程式。
    """
    global stop_event, start_button, stop_button 
    root = tk.Tk()
    root.title("Python 程式執行中")
    
    # 設定視窗大小和佈局
    root.geometry("500x400")
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(0, weight=1)

    # 建立捲動文字區域 (ScrolledText)
    text_area = scrolledtext.ScrolledText(
        root, 
        wrap=tk.WORD, 
        width=60, 
        height=15,
        font=("Consolas", 12)
    )
    text_area.grid(row=0, column=0, padx=12, pady=12, sticky="nsew")

    # 將 sys.stdout 重新導向
    sys.stdout = TextRedirector(text_area)
    
    # --- 按鈕框架 ---
    button_frame = tk.Frame(root)
    button_frame.grid(row=1, column=0, pady=(0, 10))
    
    # 建立一個線程事件，用於發送停止信號
    stop_event = threading.Event()
    
    # 建立啟動命令
    def start_program():
        # 1. 重置停止事件，準備新的運行
        stop_event.clear()
        
        # 2. 禁用開始按鈕，啟用停止按鈕
        start_button.config(state=tk.DISABLED, text="程式執行中...")
        stop_button.config(state=tk.NORMAL)

        # 3. 使用線程運行主要邏輯
        thread = threading.Thread(
            target=main_program_logic, 
            args=(start_button, stop_button, stop_event)
        )
        thread.start()

    # 建立停止命令
    def stop_program_gui():
        print("\n[信號]: 偵測到使用者請求停止...")
        # 設定停止事件，通知 main_program_logic 退出循環
        stop_event.set()
        # 立即禁用停止按鈕，防止重複點擊
        stop_button.config(state=tk.DISABLED)

    # 建立開始按鈕
    start_button = tk.Button(
        button_frame, 
        text="點擊開始執行程式", 
        command=start_program, 
        padx=20, 
        pady=5,
        bg="#4CAF50", # 綠色背景
        fg="white",  # 白色文字
        font=("Arial", 12, "bold")
    )
    start_button.pack(side=tk.LEFT, padx=10) # 靠左放置

    # 建立停止按鈕
    stop_button = tk.Button(
        button_frame, 
        text="停止程式", 
        command=stop_program_gui, 
        padx=20, 
        pady=5,
        bg="#F44336", # 紅色背景
        fg="white",  # 白色文字
        font=("Arial", 12, "bold"),
        state=tk.DISABLED # 預設禁用
    )
    stop_button.pack(side=tk.LEFT, padx=10) # 靠左放置
    
    # 啟動 Tkinter 的事件循環
    root.mainloop()

# 程式進入點
if __name__ == "__main__":
    run_gui_application()