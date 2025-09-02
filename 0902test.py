#m分鐘: 1m 
#h小時: 1h
#d天: 1d 
#wk週: 1wk 
#mo月: 1mo 
#y年: 1y

#period期間
#選項: 1d 5d 1mo 3mo 6mo 1y 2y 5y 10y

#interval間隔多久
#選項: 1m 2m 5m 15m 30m 60m 90m 
 
import yfinance as y 
import pandas as p 
import plotly.graph_objects as g 

#上市股: '股票代碼.tw'
#上櫃股: '股票代碼.two'
stock = y.download('2330.tw', period='1d', interval='1m')

print(stock)

data = stock.reset_index()
data.columns = ['現在時間', '收盤價', '最高價', '最低價', '開盤價', '成交量']
data['現在時間'] = p.to_datetime(data['現在時間'].dt.strftime('%Y-%m-%d %H:%M'))
data['成交量'] //= 1000  #把單位從成交股數換成成交張數

result = g.Figure()

#成交量的圖
result.add_trace(
    g.Bar(
        name = '成交量',
        x = data['現在時間'],
        y = data['成交量'],
        yaxis = 'y2',
        marker_color = '#99ccff'
    )
)

#k線圖
result.add_trace(
    g.Candlestick(
        name = '',
        x = data['現在時間'],
        open = data['開盤價'],
        high = data['最高價'],
        low = data['最低價'],
        close = data['收盤價'],
        increasing_line_color = '#fd5047',
        increasing_fillcolor = '#f29696',
        decreasing_line_color = '#3d9970',
        decreasing_fillcolor = '#91c2b3'
    )
)

result.update_layout(
    title = '2474可成',
    hovermode = 'x unified', #滑鼠停在圖上的時候會有資訊卡
    #xaxis_rangeslider_visible = False, #x軸座標的小圖，可以用來滑動上面的圖

    yaxis = dict(
        title = '股價'
    ),

    yaxis2 = dict(
        overlaying = 'y',
        visible = False
    ),

    font = dict(
        size = 20
    )
)

result.show()