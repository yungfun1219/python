import pandas as pd
import os

# 1. 定義檔案路徑
# 使用 r"..." (raw string) 以避免處理反斜線的轉義問題
file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\stocks_all.csv"

# 2. 檢查檔案是否存在
if not os.path.exists(file_path):
    print(f"錯誤：檔案不存在於 {file_path}")
else:
    try:
        # 3. 讀取 CSV 檔案
        # header=0 告訴 Pandas 檔案的第一行是欄位標題 (header)
        # encoding='utf-8' 確保能正確處理中文或其他非英文編碼
        df = pd.read_csv(file_path, header=0, encoding='utf-8')
        
        # 4. 計算股票數量
        # DataFrame 的 shape 屬性返回 (列數, 欄數)，我們需要的是第一個元素 (索引 0)
        stock_count = df.shape[0]
        
        # 5. 輸出結果
        print(f"成功讀取檔案：{file_path}")
        print(f"檔案中的股票總數量為：{stock_count} 支")

    except UnicodeDecodeError:
        print("錯誤：使用 UTF-8 編碼讀取失敗。嘗試使用 'big5' 編碼。")
        try:
            # 由於臺灣的金融資料有時會使用 big5 編碼
            df = pd.read_csv(file_path, header=0, encoding='big5')
            stock_count = df.shape[0]
            print(f"成功使用 BIG5 編碼讀取檔案：{file_path}")
            print(f"檔案中的股票總數量為：{stock_count} 支")
        except Exception as e:
            print(f"使用 BIG5 編碼讀取時也發生錯誤: {e}")

    except Exception as e:
        print(f"讀取或處理檔案時發生錯誤: {e}")