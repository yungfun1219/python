import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

#當前目錄
current_dir = os.path.dirname(os.path.abspath(__file__))+"\\"
stock_data_dir = current_dir + 'stock_data\\'

# 檔案路徑與日期欄位設定
FILE_PATH = r'D:\Python_repo\python\選股投資策略\stock_data\2330_台積電_stock.csv'
DATE_COLUMN_NAME = '日期'  # 請務必確認您的 CSV 檔案中，日期欄位的實際名稱！

 # 民國年轉西元年
def transform_date(date):
    Y, m, d = date.split('/')
    return str(int(Y) + 1911) + '/' + m + '/' + d

#df.to_csv(current_dir +'out.csv', index=False)    # 輸出成 CSV

# 輸出一個份肥的 DataFrame 表格與其股票代碼
def data_to_csv(input_dataframe, stocks):
    # 確認檔案是否存在，如果存在就往下執行
    if os.path.isfile(stock_data_dir + '{}_{}_stock.csv'.format(stocks, '台積電')):
        # 用異常處理讀取檔案，檢查此資料是否有問題
        try:
            cu_data = pd.read_csv(stock_data_dir + '{}_{}_stock.csv'.format(stocks, '台積電'))

            # 檢查資料是否有重複
            if input_dataframe['日期'][0] in list(cu_data['日期']):
                print('資料檢查結果：有重複資料...不重複寫入!')
                print('寫入完成！')
                time.sleep(1)
            else:
                print('資料檢查結果：無重複資料...寫入中...')
                input_dataframe.to_csv(stock_data_dir + '{}_{}_stock.csv'.format(stocks,  '台積電'), mode='a', index=False, header=False, encoding='utf-8-sig')
                print('寫入完成!')
                time.sleep(5)
                
        # 設定錯誤處理，避免跑到一半問題停止運行
        except:
            print('有某步驟錯誤，請檢察 CODE，')
            
    # 如果資料不存在，建立一份新資料
    else:
        print('建立新資料...')
        # 寫入 csv。寫入前記得先創建「stock_data」的資料夾  
        if not os.path.exists(stock_data_dir):
            os.makedirs(stock_data_dir)
        input_dataframe.to_csv(stock_data_dir + '{}_{}_stock.csv'.format(stocks,  '台積電'), mode='w', index=False, encoding='utf-8-sig')
        print('寫入完成！')
        time.sleep(5)

def process_stock_data(file_path, date_column):
    """
    讀取 CSV 檔案，統一日期格式為 YYYY/MM/DD，依日期排序，並回存到原始檔案中。
    """
    print(f"--- 開始處理檔案：{file_path} ---")

    # 1. 讀取 CSV 檔案並嘗試將日期轉換為 datetime 類型
    try:
        # Pandas 具備智能解析能力，通常能處理多種日期分隔符號 (/, - 等)
        df = pd.read_csv(file_path)
        
        # 關鍵：將日期欄位轉換為 datetime 物件，這是排序和格式化的基礎
        df[date_column] = pd.to_datetime(df[date_column])
        
    except FileNotFoundError:
        print(f"【錯誤】找不到檔案路徑：{file_path}")
        return
    except KeyError:
        print(f"【錯誤】指定的日期欄位名稱 '{date_column}' 不存在。請檢查欄位名稱是否正確。")
        # 顯示所有欄位名稱供用戶參考
        try:
            temp_df = pd.read_csv(file_path)
            print(f"檔案中的所有欄位名稱為：{temp_df.columns.tolist()}")
        except:
            pass
        return
    except Exception as e:
        print(f"【錯誤】讀取或解析日期時發生其他錯誤：{e}")
        return

    # 2. 進行日期排序 (通常股價資料是舊到新，因此使用升序)
    # 這裡選擇由 '舊到新' (升序) 排列。
    df_sorted = df.sort_values(by=date_column, ascending=True)

    # 3. 統一日期格式並重設索引
    
    # 統一格式為 YYYY/MM/DD (例如: 2025/01/01)
    df_sorted[date_column] = df_sorted[date_column].dt.strftime('%Y/%m/%d')
    
    # 重設索引 (讓左側的編號從 0 開始連續排列)
    df_sorted.reset_index(drop=True, inplace=True)
    
    print("\n--- 處理結果 ---")
    print(f"統一日期格式後，前 3 筆日期範例：{df_sorted[date_column].head(3).tolist()}")
    print("排序後資料 (前 3 筆):")
    print(df_sorted.head(3))

    # 4. 回存到原檔案
    try:
        # index=False 移除索引欄位，encoding='utf-8-sig' 確保中文編碼正確
        df_sorted.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"\n【成功】資料已統一格式、排序並回存至檔案：{file_path}")
    except Exception as e:
        print(f"【錯誤】儲存檔案時發生錯誤：{e}")

headers = {
    'content-type': 'text/html; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }

baseurl = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date=20100101&stockNo=2330"
#baseurl = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={search_year}{search_month}01&stockNo={stock_no}"

# 取得資料
data = requests.get(url=baseurl, headers=headers).content
soup = BeautifulSoup(data, 'html.parser')

# 取得表格的 thead 與 tbody
thead = soup.find('thead')
tbody = soup.find('tbody')

if thead is None or tbody is None:
    print(f'⚠ 無法取得資料，請檢查網址或日期：{baseurl}')


# 取得欄位名稱
columns = [th.text.strip() for th in thead.find_all('th')][1:]
print('欄位名稱：', columns)

# 取得表格資料
datalist = []
for tr in tbody.find_all('tr'):
    tds = [td.text.strip() for td in tr.find_all('td')]
    if tds:
        tds[0] = transform_date(tds[0])  # 轉日期
        datalist.append(tds)

# 轉成 DataFrame
df = pd.DataFrame(datalist, columns=columns)

data_to_csv(df, '2330')

#讀取 CSV 檔案，統一日期格式為 YYYY/MM/DD，依日期排序，並回存到原始檔案中。
process_stock_data(FILE_PATH, DATE_COLUMN_NAME)