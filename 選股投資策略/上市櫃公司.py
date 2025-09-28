import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# 設定請求標頭（模擬瀏覽器）
headers = {
    'content-type': 'text/html; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/76.0.3809.132 Safari/537.36'
}

# 取得證券清單的程式
def list_stock(input_type):
    # 上市與上櫃網址
    list_stocks = {
        'exchange': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',
        'counter': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
    }
    # 發送請求
    getdata = requests.get(list_stocks[input_type], headers=headers)


    # 解析網頁（注意字碼是 MS950）
    soup = BeautifulSoup(getdata.content, 'html.parser', from_encoding='MS950').find('table',class_='h4')

    # 抓取表格資料
    datalist = []
    for col in soup.find_all_next('tr'):
        datalist.append([row.text for row in col.find_all('td')])

    # 處理資料格式（去除空格與公司名稱分割）
    for deal_str in datalist[1:]:
        if len(deal_str) == 7:
            last = deal_str[0].split('\u3000')[1]
            deal_str[0] = deal_str[0].split('\u3000')[0]
            deal_str.insert(1, last)

    # 設定欄位名稱
    title = [
        '有價證券代號',
        '有價證券名稱',
        '國際證券辨識號碼(ISIN Code)',
        '上市日',
        '市場別',
        '產業別',
        'CFICode',
        '備註'
    ]

    # 轉換為 DataFrame
    df = pd.DataFrame(datalist[1:], columns=title)

    current_dir = os.path.dirname(os.path.abspath(__file__))+"\\"

    # 儲存成 CSV
    df.to_csv(f'{current_dir}{input_type}_list.csv', index=False, encoding='utf-8-sig')
    print(f'{input_type}_list.csv 已成功儲存！')

# 執行上市資料
list_stock('exchange')

# 執行上櫃資料
list_stock('counter')
