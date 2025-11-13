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

# 輔助函式：在程式結束時更新 GUI 狀態
def _update_gui_after_program_ends(start1_button, start2_button, stop_button, was_stopped, program_name):
    """
    程式結束後，線程安全地更新按鈕狀態。
    """
    if was_stopped:
        print(f"\n--- {program_name} 被使用者提前中止 ---")
    else:
        print(f"\n--- {program_name} 執行完畢 ---")

    root = start1_button.winfo_toplevel()
    # 重新啟用所有啟動按鈕
    root.after(0, lambda: start1_button.config(state=tk.NORMAL, text="執行第一個程式"))
    root.after(0, lambda: start2_button.config(state=tk.NORMAL, text="執行第二個程式"))
    # 禁用停止按鈕
    root.after(0, lambda: stop_button.config(state=tk.DISABLED))


# 2. 模擬您的程式邏輯 (在獨立線程中運行)
def program_logic_one(start1_button, start2_button, stop_button, stop_event):
    """
    第一個程式的邏輯：較快的執行。
    """
    program_name = "程式一"
    print(f"\n--- {program_name} 開始執行 (速度較快) ---")
    
    was_stopped = False
    for i in range(1, 40): 
        if stop_event.wait(0.2): # 檢查停止信號 (間隔 0.2 秒)
            was_stopped = True
            break
        
        print(f"[{program_name} 步驟 {i}] 正在快速處理數據...")
        
    # 線程安全地更新 GUI 狀態
    _update_gui_after_program_ends(start1_button, start2_button, stop_button, was_stopped, program_name)


def program_logic_two(start1_button, start2_button, stop_button, stop_event):
    """
    第二個程式的邏輯：較慢的執行。
    """
    program_name = "程式二"
    print(f"\n--- {program_name} 開始執行 (速度較慢) ---")
    
    was_stopped = False
    for i in range(1, 20): 
        if stop_event.wait(1.0): # 檢查停止信號 (間隔 1.0 秒)
            was_stopped = True
            break
        
        print(f"[{program_name} 步驟 {i}] 正在仔細處理數據...")
        
    # 線程安全地更新 GUI 狀態
    _update_gui_after_program_ends(start1_button, start2_button, stop_button, was_stopped, program_name)


# 3. 核心 GUI 建立函式
def run_gui_application():
    """
    建立並運行帶有輸出顯示器的 Tkinter GUI 應用程式。
    """
    root = tk.Tk()
    root.title("Python 程式執行器 (雙功能)")
    
    # 設定視窗大小和佈局
    root.geometry("600x450")
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(0, weight=1)

    # 建立捲動文字區域 (ScrolledText)
    text_area = scrolledtext.ScrolledText(
        root, 
        wrap=tk.WORD, 
        width=70, 
        height=18,
        font=("Consolas", 11),
        borderwidth=2,
        relief="sunken"
    )
    text_area.grid(row=0, column=0, padx=15, pady=15, sticky="nsew")

    # 將 sys.stdout 重新導向
    sys.stdout = TextRedirector(text_area)
    
    # --- 按鈕框架 ---
    button_frame = tk.Frame(root)
    button_frame.grid(row=1, column=0, pady=(0, 15))
    
    # 建立一個線程事件，用於發送停止信號
    stop_event = threading.Event()
    
    # 建立啟動命令函式 (通用啟動邏輯)
    def start_program(program_number):
        # 1. 重置停止事件，準備新的運行
        stop_event.clear()
        
        # 2. 禁用所有啟動按鈕，並標記當前執行中的程式
        start1_button.config(state=tk.DISABLED)
        start2_button.config(state=tk.DISABLED)
        stop_button.config(state=tk.NORMAL)

        if program_number == 1:
            start1_button.config(text="程式一執行中...")
            target_func = program_logic_one
        else:
            start2_button.config(text="程式二執行中...")
            target_func = program_logic_two

        # 3. 使用線程運行主要邏輯
        thread = threading.Thread(
            target=target_func, 
            args=(start1_button, start2_button, stop_button, stop_event)
        )
        thread.daemon = True 
        thread.start()

    # 建立停止命令
    def stop_program_gui():
        print("\n[信號]: 偵測到使用者請求中止執行...")
        # 設定停止事件，通知當前運行的線程退出循環
        stop_event.set()
        # 立即禁用停止按鈕
        stop_button.config(state=tk.DISABLED)

    # 建立三個按鈕
    start1_button = tk.Button(
        button_frame, 
        text="執行第一個程式", 
        command=lambda: start_program(1), 
        padx=15, 
        pady=8,
        bg="#007BFF", # 藍色背景
        fg="white",  
        font=("Arial", 11, "bold")
    )
    start1_button.pack(side=tk.LEFT, padx=10) 

    start2_button = tk.Button(
        button_frame, 
        text="執行第二個程式", 
        command=lambda: start_program(2), 
        padx=15, 
        pady=8,
        bg="#FFC107", # 黃色背景
        fg="black",  
        font=("Arial", 11, "bold")
    )
    start2_button.pack(side=tk.LEFT, padx=10) 
    
    stop_button = tk.Button(
        button_frame, 
        text="結束程式", 
        command=stop_program_gui, 
        padx=15, 
        pady=8,
        bg="#DC3545", # 紅色背景
        fg="white",  
        font=("Arial", 11, "bold"),
        state=tk.DISABLED # 預設禁用
    )
    stop_button.pack(side=tk.LEFT, padx=10) 
    
    # 啟動 Tkinter 的事件循環
    root.mainloop()

# 程式進入點
if __name__ == "__main__":
    run_gui_application()