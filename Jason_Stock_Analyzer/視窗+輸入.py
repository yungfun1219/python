import tkinter as tk
from tkinter import messagebox
import csv
import os

# --- 設定檔案名稱 ---
CSV_FILE_NAME = "user_input_log.csv"

def write_to_csv(message_title, fixed_message, user_input):
    """
    將訊息和使用者輸入的內容寫入 CSV 檔案。
    """
    try:
        # 檢查檔案是否存在，以決定是否需要寫入標題列
        file_exists = os.path.isfile(CSV_FILE_NAME)
        
        # 'a' 模式表示以「附加」（append）方式開啟，不會覆蓋舊資料
        with open(CSV_FILE_NAME, 'a', newline='', encoding='utf-8-sig') as csvfile:
            # 使用 csv.writer 處理 CSV 寫入
            csv_writer = csv.writer(csvfile)
            
            # 如果檔案不存在，則寫入標題列
            if not file_exists:
                headers = ["標題", "固定訊息", "使用者輸入內容"]
                csv_writer.writerow(headers)
            
            # 寫入資料列
            data_row = [message_title, fixed_message, user_input]
            csv_writer.writerow(data_row)
            
            return True, f"資料已成功儲存至 {CSV_FILE_NAME}"

    except Exception as e:
        return False, f"儲存檔案時發生錯誤: {e}"

def close_and_save(root, entry_widget, message_title, fixed_message):
    """
    獲取輸入內容，儲存到 CSV，然後關閉視窗。
    """
    # 1. 取得使用者在 Entry 輸入的內容
    user_data = entry_widget.get()
    
    # 2. 寫入 CSV 檔案
    success, log_message = write_to_csv(message_title, fixed_message, user_data)
    
    # 3. 顯示儲存結果（可選，用於確認操作成功）
    if success:
        print(log_message)
        # messagebox.showinfo("儲存成功", log_message)
    else:
        # 訊息框會阻擋 root.destroy()，因此我們只在主控台印出錯誤
        print(log_message)

    # 4. 關閉視窗
    root.destroy()


def display_main_window_with_input(title, fixed_message, input_label_text):
    """
    建立帶有訊息、輸入框和儲存按鈕的主視窗。
    """
    root = tk.Tk()
    root.title(title)
    
    # 設置視窗大小
    root.geometry("500x300")
    
    # --- 1. 固定訊息標籤 ---
    message_label = tk.Label(
        root, 
        text=fixed_message, 
        font=("Arial", 12), 
        wraplength=450,
        justify=tk.LEFT
    )
    message_label.pack(padx=20, pady=(20, 10), anchor='w') # anchor='w' 靠左對齊

    # --- 2. 輸入框提示標籤 ---
    input_label = tk.Label(
        root, 
        text=input_label_text, 
        font=("Arial", 12)
    )
    input_label.pack(padx=20, pady=(10, 5), anchor='w')
    
    # --- 3. 輸入框 (Entry) ---
    user_entry = tk.Entry(
        root, 
        width=60, 
        font=("Arial", 12)
    )
    user_entry.pack(padx=20, pady=(0, 20))
    
    # --- 4. 確認按鈕 (Button) ---
    confirm_button = tk.Button(
        root,
        text="確 認 並 儲 存",
        font=("Arial", 12, "bold"),
        # 點擊時，呼叫 close_and_save 函式，並傳遞所有必要參數
        command=lambda: close_and_save(root, user_entry, title, fixed_message)
    )
    confirm_button.pack(pady=10)
    
    # 啟動 Tkinter 的事件迴圈
    root.mainloop()


# --- 主要程式執行區 ---

WINDOW_TITLE = "使用者資訊輸入"
FIXED_MESSAGE = "請在下方輸入框填寫您的操作說明或備註，這將會被記錄下來。"
INPUT_PROMPT = "請輸入備註內容："

display_main_window_with_input(WINDOW_TITLE, FIXED_MESSAGE, INPUT_PROMPT)