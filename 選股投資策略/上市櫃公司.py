import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    'content-type': 'text/html; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/76.0.3809.132 Safari/537.36'
}

def list_stock(input_type):
    list_stocks = {
        'exchange': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',
        'counter': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
    }
    getdata = requests.get(list_stocks[input_type], headers=headers, verify=False)
    getdata.encoding = 'ms950'
    soup = BeautifulSoup(getdata.text, 'html.parser')
    table = soup.find('table', class_='h4')
    if table is None:
        print("找不到表格，請檢查網頁結構或class名稱")
        return

    datalist = []
    for col in table.find_all('tr'):
        datalist.append([row.text for row in col.find_all('td')])

    for deal_str in datalist[1:]:
        if len(deal_str) == 7:
            last = deal_str[0].split('\u3000')[1]
            deal_str[0] = deal_str[0].split('\u3000')[0]
            deal_str.insert(1, last)

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

    df = pd.DataFrame(datalist[1:], columns=title)
    current_dir = os.path.dirname(os.path.abspath(__file__)) + "\\"
    df.to_csv(f'{current_dir}{input_type}_list.csv', index=False, encoding='utf-8-sig')
    print(f'{input_type}_list.csv 已成功儲存！')

list_stock('exchange')
list_stock('counter')