from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, TextMessage, 
    PushMessageRequest,
    # 新增匯入 Flex Message 相關類別
    FlexMessage, QuickJson 
)
import json # 需要用到 json 函式庫來處理 JSON 字典

# 假設這裡已經定義了 messaging_api = MessagingApi(api_client)

def create_color_stock_flex(df_data: pd.DataFrame, date_to_check: str) -> FlexMessage:
    """
    根據篩選後的漲跌資料 (df_data) 建立一個 Flex Message。
    將漲跌 (+/-) 設為綠色/紅色。
    """
    
    # 設置標題和副標題
    header_contents = [
        {
            "type": "text",
            "text": f"📈 {date_to_check} 漲勢股速報",
            "weight": "bold",
            "size": "md",
            "color": "#1DB446" # 綠色標題
        }
    ]

    # 處理資料行內容
    body_contents = []
    
    # 限制顯示前 15 筆，避免訊息過長
    data_to_show = df_data.head(15) 

    # 逐筆處理每一行資料
    for index, row in data_to_show.iterrows():
        
        change_text = row['漲跌(+/-)']
        # 根據漲跌標記設定顏色
        color = "#FF0000" if change_text == '+' else "#000000" # 預設只篩選 '+'，所以這裡可能都是紅色

        content = {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {
                    "type": "text",
                    "text": f"{row['證券代號']} {row['證券名稱']}",
                    "flex": 3,
                    "size": "sm",
                    "color": "#333333",
                    "align": "start"
                },
                {
                    "type": "text",
                    "text": f"{change_text}{row['漲跌價差']} ({row['收盤價']})",
                    "flex": 2,
                    "size": "sm",
                    "color": color, # 設定顏色
                    "align": "end",
                    "weight": "bold"
                }
            ],
            "spacing": "sm",
            "margin": "sm"
        }
        body_contents.append(content)
        
    # 底部總計
    body_contents.append({
        "type": "separator",
        "margin": "md"
    })
    body_contents.append({
        "type": "text",
        "text": f"✅ 總共找到 {len(df_data)} 檔符合條件。",
        "size": "sm",
        "color": "#AAAAAA",
        "align": "center",
        "margin": "md"
    })


    # 構建完整的 Flex Bubble 結構
    flex_content = {
        "type": "bubble",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": header_contents
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": body_contents
        }
    }

    # 轉換成 FlexMessage 物件
    return FlexMessage(
        alt_text=f"{date_to_check} 漲勢股速報，共 {len(df_data)} 檔。",
        contents=QuickJson(flex_content)
    )

# ----------------------------------------------------------------------
# 替換原有的 send_stock_notification 函式
# ----------------------------------------------------------------------

def send_stock_notification(user_id, message_data: Union[str, FlexMessage]):
    """
    發送訊息的通用函式，可接受 FlexMessage 物件或純文字。
    """
    global messaging_api 
    
    try:
        if isinstance(message_data, str):
            # 如果傳入的是字串，則傳送純文字訊息
            messages_list = [TextMessage(text=message_data)]
            message_type = "純文字訊息"
        elif isinstance(message_data, FlexMessage):
            # 如果傳入的是 FlexMessage 物件，則傳送 Flex Message
            messages_list = [message_data]
            message_type = "Flex Message"
        else:
            print(f"【錯誤】不支援的訊息類型: {type(message_data)}")
            return

        push_message_request = PushMessageRequest(
            to=user_id,
            messages=messages_list
        )
        messaging_api.push_message(push_message_request) 
        print(f"{message_type} 已成功發送給 {user_id}")
    except Exception as e:
        print(f"訊息發送錯誤: {e}")


# ----------------------------------------------------------------------
# main_run 函式內的調用方式 (範例)
# ----------------------------------------------------------------------
# 在 main_run 函式中，當您取得 result (df_filtered) 後，您可以這樣調用：

def main_run():
    # ... (您的抓取和篩選程式碼) ...
    # 假設 result 是篩選後的 DataFrame
    # 假設 DATE_TO_CHECK 是 '2025/10/07'
    
    if not result.empty:
        # 1. 建立帶有顏色和格式的 Flex Message 物件
        flex_message_object = create_color_stock_flex(result, DATE_TO_CHECK)
        
        # 2. 發送 Flex Message
        send_stock_notification(LINE_USER_ID, flex_message_object)
    else:
        # 3. 如果沒有資料，則傳送純文字訊息
        no_data_message = f"{DATE_TO_CHECK} 未找到符合所有篩選條件的資料。"
        send_stock_notification(LINE_USER_ID, no_data_message)
    
    # ... (其他程式碼) ...