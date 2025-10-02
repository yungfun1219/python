#def get_yield():
#import pandas as pd

#html_url = 'https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date=20180101&selectType=ALL'
#table = pd.read_html(html_url)
#print(table)

import pandas as pd
from urllib.error import HTTPError

# 範例 URL，您可以替換成迴圈中的變數
html_url = 'https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date=20180102&selectType=ALL'

def read_twse_table(url):
    """
    嘗試讀取 TWSE 網頁上的表格，如果網頁沒有資料則略過。
    """
    print(f"嘗試讀取 URL: {url}")
    
    # 使用 try-except 捕獲可能發生的錯誤
    try:
        # 1. 嘗試讀取網頁表格
        # 如果網頁結構不符合預期（例如無資料時），pd.read_html 會拋出 ValueError
        table = pd.read_html(url)
        
        # 2. 檢查回傳的列表是否為空
        if not table:
            print("【略過】網頁成功載入，但未包含任何表格資料 (回傳列表為空)。")
            return None
        
        # 3. 成功獲取資料
        df = table[0] # 假設所需的表格是列表中的第一個元素
        print("【成功】資料已載入，前五筆資料如下：")
        print(df.head())
        
        return df

    except ValueError:
        # 當網頁無資料或表格結構不符時，通常拋出 ValueError
        print("【略過】網頁內容可能無資料或找不到預期的 HTML 表格結構。")
        return None
    
    except HTTPError as e:
        # 處理網路請求錯誤，例如 404/500 錯誤
        print(f"【錯誤】網路請求失敗，HTTP 錯誤碼: {e.code}")
        return None
    
    except Exception as e:
        # 處理其他所有未預期的錯誤
        print(f"【錯誤】發生未預期的錯誤: {e}")
        return None


# 執行函式
df_data = read_twse_table(html_url)

# 您可以在這裡對 df_data 進行後續處理
if df_data is not None:
    print("\n資料處理完畢，準備進行下一步分析...")