import tkinter as tk

def display_main_window_with_button(title, message):
    """
    建立一個自定義的主視窗，並包含一個可關閉視窗的確認按鈕。
    """
    root = tk.Tk()
    root.title(title)
    
    # 設置視窗大小 (寬 x 高)
    root.geometry("400x250") 
    
    # 1. 建立訊息標籤 (Label)
    message_label = tk.Label(
        root, 
        text=message, 
        font=("Arial", 14), 
        wraplength=350,      
        justify=tk.CENTER    # 文字置中對齊
    )
    
    # 將標籤放置在視窗上半部，使用 side='top'
    # pady=20 給予垂直邊距
    message_label.pack(padx=20, pady=40)
    
    # 2. 建立確認按鈕 (Button)
    confirm_button = tk.Button(
        root,
        text="確 認",
        font=("Arial", 12, "bold"),
        # 當按鈕被點擊時，呼叫 root.destroy 函式
        # root.destroy() 會關閉並銷毀整個視窗
        command=root.destroy 
    )
    
    # 將按鈕放置在視窗底部
    confirm_button.pack(pady=20)
    
    # 啟動 Tkinter 的事件迴圈，讓視窗保持開啟狀態
    root.mainloop()


# --- 主要程式執行區 ---

# 您要傳遞的訊息和標題
WINDOW_TITLE = "程式執行通知"
TRANSMIT_MESSAGE = "親愛的用戶：\n您的程式已經成功移轉並開始執行！\n請按「確認」繼續。"

# 呼叫帶有按鈕的新函式
display_main_window_with_button(WINDOW_TITLE, TRANSMIT_MESSAGE)