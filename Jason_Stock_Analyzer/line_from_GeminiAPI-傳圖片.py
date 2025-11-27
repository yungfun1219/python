import os
from dotenv import load_dotenv 
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
# ➊ 匯入 ImageMessage
from linebot.v3.messaging import TextMessage, ImageMessage, PushMessageRequest


# ➋ 載入 line_API.env 檔案中的變數
# 注意：如果您使用 .env 以外的檔名 (如 line_token.env)，需要指定檔名
load_dotenv(r"D:\Python_repo\python\Jason_Stock_Analyzer\line_API.env") 

# ➌ 從環境變數中讀取 Token 和 User ID
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_USER_ID = os.getenv("LINE_USER_ID")

# ----------------- 檢查 Token 是否存在 -----------------
if not LINE_CHANNEL_ACCESS_TOKEN:
    print("錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
    exit()
# ----------------------------------------------------

try:
    # 初始化 Configuration 和 MessagingApi
    configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
    api_client = ApiClient(configuration)
    messaging_api = MessagingApi(api_client)
    print("Line Bot API 初始化成功。")
except Exception as e:
    print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")
    
# 修正 LineBotApiError 的匯入路徑（根據您上一個問題的解答）
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
from linebot.v3.messaging import TextMessage, PushMessageRequest


# ----------------- 檢查 Token 是否存在 -----------------
if not LINE_CHANNEL_ACCESS_TOKEN:
    print("錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
    exit()
# ----------------------------------------------------


try:
    # 初始化 Configuration 和 MessagingApi
    configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
    api_client = ApiClient(configuration)
    messaging_api = MessagingApi(api_client)
    print("Line Bot API 初始化成功。")
except Exception as e:
    print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")

# ... 接下來的程式碼保持不變 ...
# 這是接收訊息的用戶 ID 或群組 ID
# LINE_USER_ID 現在已經從 .env 檔案中讀取

def send_stock_notification(user_id, message_text):
    try:
        push_message_request = PushMessageRequest(
            to=user_id,
            messages=[TextMessage(text=message_text)]
        )
        # 注意：這裡使用全域變數 messaging_api，如果初始化失敗，這裡會報錯
        messaging_api.push_message(push_message_request) 
        print(f"訊息已成功發送給 {user_id}")
    except Exception as e:
        print(f"其他錯誤: {e}")

# ----------------------------------------------------
# ➋ 新增：傳送圖片訊息的函式
# ----------------------------------------------------
def send_image_notification(user_id, image_url, preview_url):
    """使用 Line Messaging API 傳送圖片訊息。
    image_url 和 preview_url 必須是 HTTPS 網址。"""
    try:
        # 建立 ImageMessage 物件 (PNG 檔案適用)
        image_message = ImageMessage(
            original_content_url=image_url,
            preview_image_url=preview_url
        )

        # 建立 PushMessageRequest
        push_message_request = PushMessageRequest(
            to=user_id,
            messages=[image_message]
        )

        messaging_api.push_message(push_message_request) 
        print(f"圖片 (PNG) 已成功發送給 {user_id}")
    except Exception as e:
        print(f"發送圖片時發生錯誤: {e}")
        
# 範例執行
# 這裡使用一個明確的佔位符
IMAGE_URL = "https://png.pngtree.com/thumb_back/fh260/background/20240412/pngtree-happy-small-dog-is-running-on-a-grass-in-park-in-image_15655854.jpg"
PREVIEW_URL = IMAGE_URL # 暫時使用原圖作為預覽圖

analysis_report = "🥇✅台積電⬆️ (2330)🎯 🟢近期🔴走勢強勁，RSI ⚠️1️⃣位於 65，預⭐期🚨短期內仍有上漲動能。"
send_stock_notification(LINE_USER_ID, analysis_report)

# 2. 傳送圖片訊息
if "YOUR-PUBLIC-IMAGE-HOSTING.COM" in IMAGE_URL:
    print("\n⚠️ 警告：您尚未設定圖片 URL。請務必將 PNG 圖片上傳到網路並替換 IMAGE_URL 後再執行！")
else:
    send_image_notification(LINE_USER_ID, IMAGE_URL, PREVIEW_URL)