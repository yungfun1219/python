import requests
from bs4 import BeautifulSoup
res = requests.get('https://news.cnyes.com/news/cat/tw_stock_news').content
soup = BeautifulSoup(res, 'html.parser')

go = soup.find('div',{'class':'_2bFl theme-list'})

basic = 'https://news.cnyes.com'

print('最新台股新聞')
news =[]

for i in range(5):
    news.append(
        [
            go.find_all('a')[i].text,
            basic + go.find_all('a')[i]['href']
        ]
    )