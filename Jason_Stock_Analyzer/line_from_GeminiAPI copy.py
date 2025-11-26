import os
from dotenv import load_dotenv # ➊ 匯入函式庫
import google.generativeai as genai



# ➋ 載入 line_API.env 檔案中的變數
# 注意：如果您使用 .env 以外的檔名 (如 line_token.env)，需要指定檔名
load_dotenv(r"D:\Python_repo\python\Jason_Stock_Analyzer\line_API.env") 

# ➌ 從環境變數中讀取 Token 和 User ID
# LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
# LINE_USER_ID = os.getenv("LINE_USER_ID")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)

# 2. 選擇模型
# 目前常用模型：'gemini-1.5-flash' (快速、便宜) 或 'gemini-1.5-pro' (強大)
model = genai.GenerativeModel('gemini-2.5-pro')

# 3. 發送提示詞 (Prompt)
response = model.generate_content(
                """ 
                1.請在台灣證券交易所抓取華邦電(2344.TW)，時間為2025/11/25的股價
                2.提供該股票的未來趨勢，以100字內做總結。
                3.請提供AI模型的現在時間!
                """
                                  )

# 4. 印出結果
print(response.text)

# # 修正 LineBotApiError 的匯入路徑（根據您上一個問題的解答）
# from linebot.v3.messaging import Configuration, ApiClient, MessagingApi
# from linebot.v3.messaging import TextMessage, PushMessageRequest


# # ----------------- 檢查 Token 是否存在 -----------------
# if not LINE_CHANNEL_ACCESS_TOKEN:
#     print("錯誤：LINE_CHANNEL_ACCESS_TOKEN 未在 line_API.env 中設置或讀取失敗。程式中止。")
#     exit()
# # ----------------------------------------------------


# try:
#     # 初始化 Configuration 和 MessagingApi
#     configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
#     api_client = ApiClient(configuration)
#     messaging_api = MessagingApi(api_client)
#     print("Line Bot API 初始化成功。")
# except Exception as e:
#     print(f"Line Bot API 初始化失敗，請檢查 Token：{e}")

# # ... 接下來的程式碼保持不變 ...
# # 這是接收訊息的用戶 ID 或群組 ID
# # LINE_USER_ID 現在已經從 .env 檔案中讀取

# def send_stock_notification(user_id, message_text):
#     try:
#         push_message_request = PushMessageRequest(
#             to=user_id,
#             messages=[TextMessage(text=message_text)]
#         )
#         # 注意：這裡使用全域變數 messaging_api，如果初始化失敗，這裡會報錯
#         messaging_api.push_message(push_message_request) 
#         print(f"訊息已成功發送給 {user_id}")
#     except Exception as e:
#         print(f"其他錯誤: {e}")

# # 範例執行
# analysis_report = "🥇✅台積電⬆️ (2330)🎯 🟢近期🔴走勢強勁，RSI ⚠️1️⃣位於 65，預⭐期🚨短期內仍有上漲動能。"
# send_stock_notification(LINE_USER_ID, analysis_report)