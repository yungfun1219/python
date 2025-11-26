import google.generativeai as genai
import os

import google.generativeai
print(google.generativeai.__version__)

# --- 設定 API Key (假設您已經設定好) ---
# 請將 YOUR_API_KEY 替換成您的實際金鑰
API_KEY = "AIzaSyBpWmKhD2iKZuUj7ZGE0pqP-igPRqtZBwc" 
genai.configure(api_key=API_KEY)

MODEL_NAME = "gemini-2.5-flash" 
prompt = "請幫我總結一下今天的國際新聞。"

try:
    # 移除 client = genai.Client()

    # 1. 初始化模型 (使用位置參數，而非 model=)
    gemini_model = genai.GenerativeModel(MODEL_NAME) 

    # 2. 呼叫 generate_content，使用 config 參數
    #    (請務必確保 SDK 是最新版，否則 config 參數仍會報錯)
    response = gemini_model.generate_content(
        contents=prompt,
        config={
            "tools": [{"google_search": {}}] 
        }
    )
    print(response.text)

except Exception as e:
    # 如果您依然遇到 config 錯誤，請再次確認您的 SDK 版本
    print(f"❌ 發生錯誤: {e}")