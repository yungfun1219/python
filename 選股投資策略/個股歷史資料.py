import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

symboldict = {
    '2330':'台積電',
    '2317':'鴻海',
    '2454':'聯發科',
    '2412':'中華電',
    '6505':'台塑化',
    '2882':'國泰金',
    '1301':'台塑',
    '2308':'台達電',
    '1303':'南亞',
    '3008':'大立光',
}

def transform_date(date): # 民國轉西元
    Y, m, d = date.split('/')
    return str(int(Y) + 1911) + '/' + m + '/' + d

def get_data(begin, stocks):
    print('\n(*) 開始蒐集資料...')
    # 爬上 header
    headers = {
        'content-type': 'text/html; charset=UTF-8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'
    }

    # 目標網址
    baseurl = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={}&stockNo={}".format(begin, stocks)

    # 加上 content 解決某些回傳失敗的問題
    data = requests.get(url=baseurl, headers=headers).content
    title = BeautifulSoup(data, 'html.parser').find('thead').find('tr')

    # 上一節爬表格的做法
    datalist = []
    for col in title.find_all_next('tr'):
        datalist.append([row.text for row in col.find_all('td')])

    # 把第一行不需要的數據拿掉
    for each in datalist[1:]:
        each[0] = transform_date(each[0])

    # 轉成 pandas 的 DataFrame
    df = pd.DataFrame(datalist[1:], columns=datalist[0])
    df.columns = datalist[0]

    print('{} {} 資料蒐集成功!!!'.format(stocks, symboldict[stocks], begin))
    # 讓 call 函數取得運算完的資料
    return df

get_data('2021/01/01', '2330')

# 輸出一個份肥的 DataFrame 表格與其股票代碼
def data_to_csv(input_dataframe, stocks):
    # 確認檔案是否存在，如果存在就往下執行
    if os.path.isfile('stock_data/{}{}.csv'.format(stocks, symboldict[stocks])):
        # 用異常處理讀取檔案，檢查此資料是否有問題
        try:
            cu_data = pd.read_csv('stock_data/{}{}.csv'.format(stocks, symboldict[stocks]))
            
            # 檢查資料是否有重複
            if input_dataframe['日期'][0] in list(cu_data['日期']):
                print('資料檢查結果：有重複資料...不重複寫入!')
                print('寫入完成！')
                time.sleep(1)
            else:
                print('資料檢查結果：無重複資料...寫入中...')
                input_dataframe.to_csv('stock_data/{}{}.csv'.format(stocks, symboldict[stocks]), mode='a', header=False)
                print('寫入完成!')
                time.sleep(5)
                
        # 設定錯誤處理，避免跑到一半問題停止運行
        except:
            print('有某步驟錯誤，請檢察 CODE，')
            
    # 如果資料不存在，建立一份新資料
    else:
        print('建立新資料...')
        # 寫入 csv。寫入前記得先創建「stock_data」的資料夾
        input_dataframe.to_csv('stock_data/{}{}.csv'.format(stocks, symboldict[stocks]), mode='w')
        print('寫入完成！')
        time.sleep(5)


# 開始的西元年分、開始的月份、結束的西元年分、結束的月份
def diff_datetime(start_year, start_month, end_year, end_month):
    year_list = []
    # 產生要執行的年份清單
    for i in range(end_year - start_year + 1):
        year_list.append(start_year + i)
        
    whole_date = []
    
    for strtime in year_list:
        # 因為開始與結束的月份不一定剛好12個月，故要另外處理
        if strtime == start_year:
            # 處理開始年的月份
            # 月份小於 10 需要在前面位數補 0，例如：3月要填 03
            for mon in range(start_month, 13):
                if mon > 9:
                    str_sm = mon
                elif mon <= 9:
                    str_sm = '0{}'.format(mon)
                else:
                    print('請輸入正確的月份：(1-12)')

                whole_date.append('{}{}{}'.format(strtime, str_sm, '01'))
                
        # 照上面的邏輯處理結束年的資料
        elif strtime == end_year:
            # 處理結束年的月份
            for mon in range(1, end_month + 1):
                if mon > 9:
                    end_sm = mon
                elif mon <= 9:
                    end_sm = '0{}'.format(mon)
                else:
                    print('請輸入正確的月份：(1-12)')
                    
                whole_date.append('{}{}{}'.format(strtime, end_sm, '01'))
                
        else:
            for nor_mon in range(1, 13):
                if nor_mon > 9:
                    nor_m = nor_mon
                elif nor_mon <= 9:
                    nor_m = '0{}'.format(nor_mon)
                else:
                    print('請輸入正確的月份：(1-12)')
                    
                whole_date.append('{}{}{}'.format(strtime, nor_m, '01'))
                
    # 輸出正式年月清單
    return whole_date

code = ['2330'] # 這個清單填入股票代碼，這份清單呼應代碼名稱轉換的清單
cralwer_date = diff_datetime(2021, 1, 2021, 5)

for sn in code:
    for dn in cralwer_date:
        # 這樣就大功告成
        try:
            data_to_csv(get_data(dn, sn), sn)
            time.sleep(5)
        except:
            # 爬到最後可能會多爬一個月的資料，所以 pass 即可
            pass