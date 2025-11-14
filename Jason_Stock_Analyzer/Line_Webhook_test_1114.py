from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# === 替換成你的 LINE Bot 資訊 ===
CHANNEL_ACCESS_TOKEN = "clq7PYXFu5m4aUrXUpKsK0ko07RPBffLFtS22/CZwZ3EKHDvfWLFxc4OQ/ClL59K9aoWyMrNVh0D0qdJNc8EuuoK6+znz69XPQo+yffzdw1Oh+wveoVOotGPPDLy92twKsxME/Y835aZCQIxcZeW8QdB04t89/1O/w1cDnyilFU="
CHANNEL_SECRET = "985ac51403046973d3c85dcedf335254"

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

@app.route("/webhook", methods=['POST'])
def webhook():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

# 接收文字訊息並回覆
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    user_msg = event.message.text

    print("➡️ UserID:", user_id)
    print("➡️ 訊息:", user_msg)

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"你剛剛說：{user_msg}")
    )

if __name__ == "__main__":
    app.run(port=3000)
