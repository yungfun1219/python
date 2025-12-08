import pandas as pd
import pathlib
# 匯入 datetime 模組來獲取當前日期
from datetime import datetime

def process_trading_days():
    """
    功能：讀取指定的 CSV 檔案，刪除「今天」以前的日期資料 (即保留今天或之後的資料)，
          並使用讀取成功的編碼器將結果寫回原檔案。
    - 整合了所有功能，遵循流程扁平化。
    """
    
    # --- 1. 定義常量和路徑 (使用 Pathlib) ---
    # ❗️ 警告：請確認此路徑確實存在且檔案可寫入
    FILE_PATH = pathlib.Path(r'D:\Python_repo\python\Jason_Stock_Project\datas\processed\get_holidays\trading_day_2021-2025.csv')
    DATE_COLUMN_NAME = '日期' 
    ENCODINGS_TO_TRY = ['utf-8', 'big5', 'cp950', 'latin-1']
    successful_encoding = None  # 用來儲存成功的編碼器
    
    # --- 2. 獲取截止日期 (動態獲取「今天」的日期) ---
    # 截止日期設為今天的日期 (零點)，保留日期 >= 今天的資料。
    # 使用 pd.Timestamp.now().normalize() 確保時間部分是 00:00:00
    cutoff_date = pd.to_datetime(datetime.now().date())
    
    print(f"✅ 篩選截止日期 (保留 >= 此日期)：{cutoff_date.strftime('%Y-%m-%d')}")
    
    # --- 3. 多編碼嘗試讀取 CSV 檔案 (並記錄成功的編碼器) ---
    df = None
    print(f"ℹ️ 嘗試讀取檔案: {FILE_PATH}")
    
    for encoding in ENCODINGS_TO_TRY:
        try:
            # 確保讀取時不會將日期解析為 index
            df = pd.read_csv(FILE_PATH, encoding=encoding)
            successful_encoding = encoding  # 記錄成功的編碼器
            print(f"🎉 成功使用編碼 '{successful_encoding}' 讀取檔案。")
            break  # 讀取成功，跳出迴圈
        except UnicodeDecodeError:
            print(f"⚠️ 編碼 '{encoding}' 失敗。")
        except FileNotFoundError:
            print(f"❌ 錯誤：找不到檔案路徑: {FILE_PATH}")
            return
        except Exception as e:
            print(f"❌ 讀取時發生未知錯誤 (編碼: {encoding}): {e}")
            return
    
    if df is None:
        print("🔴 程式終止：所有編碼嘗試均失敗，無法讀取資料。")
        return

    # --- 4. 數據檢查與轉換 ---
    original_rows = len(df)
    print(f"ℹ️ 原始資料筆數：{original_rows} 筆")

    if DATE_COLUMN_NAME not in df.columns:
        print(f"❌ 錯誤：CSV 檔案中找不到名為 '{DATE_COLUMN_NAME}' 的日期欄位。")
        return

    # 將日期欄位轉換為 datetime 物件，errors='coerce' 可將無效值轉為 NaT
    df[DATE_COLUMN_NAME] = pd.to_datetime(df[DATE_COLUMN_NAME], errors='coerce')
    
    # 排除轉換失敗 (NaT) 的行
    df = df.dropna(subset=[DATE_COLUMN_NAME])
    
    # --- 5. 執行核心日期篩選 (已修改邏輯) ---
    # 篩選邏輯：保留日期在截止日期（今天）或之後的資料
    # 刪除今天以前的資料
    df_filtered = df[df[DATE_COLUMN_NAME] >= cutoff_date]
    
    # --- 6. 輸出處理結果 ---
    rows_deleted = original_rows - len(df_filtered)
    # 由於可能排除 NaT，這裡的 rows_deleted 可能包含 NaT 的行數，但核心是展示篩選後的結果
    print(f"ℹ️ 刪除筆數 (今天 '{cutoff_date.strftime('%Y-%m-%d')}' 之前)：{rows_deleted} 筆")
    print(f"ℹ️ 處理後剩餘筆數：{len(df_filtered)} 筆")
    
    # --- 7. 寫入檔案（使用成功的編碼器覆蓋原檔案） ---
    try:
        if df_filtered.empty:
            print("⚠️ 警告：DataFrame 為空，未執行寫入操作。")
            return
        
        # 寫檔時使用 successful_encoding
        print(f"ℹ️ 準備使用 '{successful_encoding}' 編碼寫入檔案...")
        # index=False 避免將 DataFrame 的 index 寫入 CSV
        df_filtered.to_csv(FILE_PATH, index=False, encoding=successful_encoding)
        
        print(f"🎉 成功！篩選後的資料已儲存回檔案: {FILE_PATH} (使用編碼: {successful_encoding})")
    except Exception as e:
        print(f"❌ 寫入檔案時發生錯誤: {e}")


# 執行單一流程函式
if __name__ == "__main__":
    process_trading_days()