import re
import os

# 1. 定義檔案路徑和目標字串
file_path = r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\logs\last_get_date.log"
target_prefix = "最後日期1=="

# 2. 檢查檔案是否存在
if not os.path.exists(file_path):
    print(f"錯誤：檔案不存在於 {file_path}")
else:
    try:
        # 3. 開啟並讀取檔案內容
        # 使用 'r' 模式讀取，並使用 with open 確保檔案會自動關閉
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 4. 構造正規表達式
        # re.escape() 確保目標前綴中的特殊字元（如冒號）被正確處理
        # \s* 匹配零個或多個空白字元
        # (\d+) 匹配一個或多個數字，並將其作為一個群組 (Group) 捕獲
        pattern = re.escape(target_prefix) + r"\s*(\d+)"
        
        # 5. 搜尋匹配項
        match = re.search(pattern, content)

        if match:
            # 6. 提取匹配到的數字群組 (Group 1)
            # match.group(1) 提取括號 (\d+) 內捕獲的數字字串
            date_number_string = match.group(1)
            print(f"成功擷取到的日期數字字串為: {date_number_string}")
            
            # 如果需要將其轉換為整數 (Integer)，可以使用 int()
            # date_number_int = int(date_number_string)
            # print(f"轉換為整數為: {date_number_int}")
            
        else:
            print(f"錯誤：在檔案中找不到符合 '{target_prefix}' 模式的數字。")

    except Exception as e:
        print(f"讀取或處理檔案時發生錯誤: {e}")