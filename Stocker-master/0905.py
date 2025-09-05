import yfinance as yf

# 獲取 Apple 股票的數據
apple = yf.Ticker("AAPL")

# 獲取 Apple 股票的歷史價格
hist = apple.history(period="1mo")

# 顯示數據
print(hist)