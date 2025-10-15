import os
from dotenv import load_dotenv # ➊ 匯入函式庫
from google import genai
from google.genai.errors import APIError

# ➋ 載入 .env 檔案中的環境變數
load_dotenv(r'D:\Python_repo\python\Jason_Stock_Analyzer\line_API.env') 

def get_gemini_response(prompt_text):
    # 此處不再需要 os.getenv()，因為 load_dotenv() 已經將金鑰載入到系統環境變數中
    try:
        # ➌ Client() 會自動偵測並使用環境變數中的 GEMINI_API_KEY
        client = genai.Client() 
        
        # ... 後續程式碼與之前範例相同 ...
        model_name = 'gemini-2.5-flash'
        print(f"\n--- 正在使用 {model_name} 詢問 Gemini ---")
        
        response = client.models.generate_content(
            model=model_name,
            contents=prompt_text
        )
        return response.text

    except APIError as e:
        return f"Gemini API 錯誤: {e}"
    except Exception as e:
        return f"發生錯誤: {e}"

if __name__ == "__main__":
    # 檢查是否成功載入金鑰（可選）
    if not os.getenv("GEMINI_API_KEY"):
         print("錯誤：未能從 gemini_API.env 載入 GEMINI_API_KEY。請檢查檔案內容和名稱。")
         exit()
    user_input = "今天是2025年10月15日，請幫我從https://tw.stock.yahoo.com/的網站資訊，針對台灣股票台達電的技術資料！"
    # user_input = input("請輸入您的詢問文字: \n")
    if user_input.strip():
        gemini_reply = get_gemini_response(user_input)
        print("\n================== Gemini 回覆 ==================")
        print(gemini_reply)
        print("=================================================")
    else:
        print("輸入不能為空。")