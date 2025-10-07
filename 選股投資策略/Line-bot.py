from linebot import LineBotApi

# 替換成您在 LINE Developers Console 獲取的 Channel Access Token
LINE_CHANNEL_ACCESS_TOKEN = "YOUR_LINE_CHANNEL_ACCESS_TOKEN"

try:
    # 初始化 LineBotApi 客戶端
    line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
    print("Line Bot API 初始化成功。")
except Exception as e:
    print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")