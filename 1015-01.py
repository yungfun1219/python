import pandas as pd

# 檔案路徑
file_path = r'D:\Python_repo\python\選股投資策略\holidaySchedule_114.csv'

# 第 1 欄位的索引是 0
column_index = 0

try:
    # 1. 解決編碼問題：使用 encoding='big5' 讀取檔案
    # 2. 僅讀取第 1 欄 (索引 0)：使用 usecols=[column_index]
    # 3. 假設檔案沒有標題列 (header=None)，如果第一列是標題，請將 header 設為 0
    df = pd.read_csv(
        file_path, 
        encoding='big5', 
        usecols=[column_index],
        header=None
    )
    
    # 為讀取到的單一欄位命名
    df.columns = ['原始日期']
    
    # 4. **將欄位轉換為日期時間物件**
    # errors='coerce' 會將無法解析的字串設為 NaT (非時間)
    df['日期物件'] = pd.to_datetime(df['原始日期'], errors='coerce')
    
    # **處理中文日期字串的額外邏輯：**
    # 如果您的日期欄位包含中文 (例如 '2023年10月15日' 或 '2023/10/15(星期三)')，
    # 則需要先進行字串清理，才能讓 pd.to_datetime 成功解析。
    if df['日期物件'].isnull().all():
        # 如果上一步轉換失敗，則進行中文清理
        print("注意：日期欄位無法自動解析，嘗試進行中文日期字串清理...")
        
        # 移除常見的中文年月日和括號內容，以利解析
        cleaned_series = df['原始日期'].astype(str).str.replace('年', '/').str.replace('月', '/').str.replace('日', '').str.split(' ').str[0]
        
        df['日期物件'] = pd.to_datetime(cleaned_series, errors='coerce')

    
    # 5. **提取「月/日」並轉為字串格式**
    # 使用 .dt.strftime('%m/%d') 格式化日期，只保留月 (帶前導零) 和日：
    df['月日'] = df['日期物件'].dt.strftime('%m/%d')
    
    # 移除因為轉換失敗而產生的 NaT/nan
    df['月日'] = df['月日'].fillna('') 

    
    # 6. 顯示結果
    print("\n--- 成功提取第 1 欄位的「月/日」結果 (前5筆) ---")
    # to_string(index=False) 用於隱藏 DataFrame 的索引，使輸出更乾淨
    print(df[['月日']].head().to_string(index=False))
    print("-----------------------------------------------------")

except FileNotFoundError:
    print(f"錯誤：找不到檔案路徑 {file_path}")
except Exception as e:
    print(f"讀取或處理檔案時發生錯誤: {e}")