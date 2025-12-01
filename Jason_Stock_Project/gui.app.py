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
            
            return True, f"資料已成功儲存至 {CSV_FILE_NAME}\n輸入內容: {user_input}"

    except Exception as e:
        return False, f"儲存檔案時發生錯誤: {e}"

# 創建一個列表用於儲存執行結果 
execution_result = [False, "程式未執行"] 


def close_and_save(root, entry_widget, message_title, fixed_message):
    """
    獲取輸入內容，儲存到 CSV，更新全域結果，然後關閉視窗。
    """
    global execution_result
    
    # 1. 取得使用者在 Entry 輸入的內容
    user_data = entry_widget.get()
    
    # 2. 寫入 CSV 檔案
    success, log_message = write_to_csv(message_title, fixed_message, user_data)
    
    # 3. 更新全域結果變數
    execution_result[0] = success
    execution_result[1] = log_message
    
    # 4. 顯示儲存結果（可選，用於確認操作成功）
    if success:
        print(f"操作成功: {log_message}")
    else:
        print(f"操作失敗: {log_message}")

    # 5. 關閉視窗
    root.destroy()


def display_main_window_with_input(title, fixed_message, input_label_text):
    """
    建立帶有訊息、輸入框和儲存按鈕的主視窗，並回傳執行結果。
    視窗會顯示在螢幕正中央。
    """
    root = tk.Tk()
    root.title(title)
    
    # 設置視窗的固定尺寸
    window_width = 500
    window_height = 300
    
    # 【修正點】: 計算螢幕中央座標
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    
    # 計算起始 X 和 Y 座標
    center_x = int((screen_width / 2) - (window_width / 2))
    center_y = int((screen_height / 2) - (window_height / 2))
    
    # 設置視窗大小和位置 (格式: "寬x高+X+Y")
    root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    
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
    
    default_value = "20251128" # 預設顯示的數字
    user_entry.insert(0, default_value)
    
    # --- 4. 確認按鈕 (Button) ---
    confirm_button = tk.Button(
        root,
        text="確 認 並 執 行",
        font=("Arial", 12, "bold"),
        # 點擊時，呼叫 close_and_save 函式，並傳遞所有必要參數
        command=lambda: close_and_save(root, user_entry, title, fixed_message)
    )
    confirm_button.pack(pady=10)
    
    
    # 啟動 Tkinter 的事件迴圈，此處會阻塞直到 root.destroy() 被呼叫
    root.mainloop()
    
    # 回傳在 close_and_save 中被更新的全域結果
    return execution_result


# --- 主要程式執行區 ---

WINDOW_TITLE = "使用者資訊輸入"
FIXED_MESSAGE = "請輸入程式執行所需抓取資料時間，或預設今日日期。"
INPUT_PROMPT = "(例如20251128)"

# 呼叫主視窗，並在主視窗關閉後，接收執行結果
final_status, final_message = display_main_window_with_input(WINDOW_TITLE, FIXED_MESSAGE, INPUT_PROMPT)


# --- 執行結果提示小視窗 ---
if final_status:
    # 成功
    messagebox.showinfo(
        "程式執行完畢", 
        f"✅ 程式執行完畢！\n\n{final_message}"
    )
else:
    # 失敗
    messagebox.showerror(
        "程式執行失敗", 
        f"❌ 程式執行發生錯誤！\n\n錯誤訊息: {final_message}"
    )

print("\n🎉 整個應用程式流程完成！")