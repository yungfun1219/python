import os
from dotenv import load_dotenv
from openai import OpenAI
from openai import APIError # 匯入 OpenAI 的錯誤類別

# 載入 .env 檔案中的環境變數
# 注意：這裡假設您將金鑰存在 chatgpt_API.env 檔案中
load_dotenv(r'D:\Python_repo\python\Jason_Stock_Analyzer\line_API.env') 

# 檢查是否成功載入金鑰
if not os.getenv("OPENAI_API_KEY"):
    print("錯誤：找不到 OPENAI_API_KEY 環境變數。請檢查 chatgpt_API.env 檔案。")
    exit()

def get_chatgpt_response(prompt_text):
    """
    使用 OpenAI Chat Completions API 取得回覆。

    Args:
        prompt_text (str): 傳送給模型的詢問文字。

    Returns:
        str: 模型的回覆文字，如果發生錯誤則返回錯誤訊息。
    """
    try:
        # 初始化 OpenAI 用戶端
        # Client() 會自動從環境變數 OPENAI_API_KEY 讀取金鑰
        client = OpenAI()

        # 指定要使用的模型
        model_name = "gpt-3.5-turbo"

        # Chat Completions API 需要傳入一個包含對話歷史的 messages 列表
        messages_history = [
            # System 訊息：設定模型的行為
            {"role": "system", "content": "您是一位樂於助人且簡潔的 AI 助理。"},
            # User 訊息：包含使用者的提問
            {"role": "user", "content": prompt_text}
        ]

        print(f"\n--- 正在使用 {model_name} 詢問 ChatGPT ---")

        # 呼叫 Chat Completions API
        completion = client.chat.completions.create(
            model=model_name,
            messages=messages_history,
            max_tokens=500  # 限制回覆長度以節省費用
        )

        # 從回覆物件中取得文字內容
        # ChatGPT 的回覆通常在 choices[0].message.content 中
        return completion.choices[0].message.content.strip()

    except APIError as e:
        return f"ChatGPT API 錯誤: {e}"
    except Exception as e:
        return f"發生錯誤: {e}"

if __name__ == "__main__":
    
    # 1. 輸入文字
    user_input = input("請輸入您的詢問文字 (例如：用三句話解釋什麼是黑洞): \n")

    if not user_input.strip():
        print("輸入不能為空。程式結束。")
    else:
        # 2. 詢問 ChatGPT 取得回覆
        chatgpt_reply = get_chatgpt_response(user_input)

        # 3. 顯示於畫面
        print("\n================== ChatGPT 回覆 ==================")
        print(chatgpt_reply)
        print("==================================================")