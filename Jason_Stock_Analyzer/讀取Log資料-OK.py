import os
from pathlib import Path

# 1. 定義檔案路徑
file_path_str = r"D:\Python_repo\python\Jason_Stock_Project\datas\logs\config.log" # 使用 r"" (raw string) 避免反斜線的轉義問題
file_path = Path(file_path_str)

# 確保檔案存在
if not file_path.exists():
    print(f"錯誤：找不到檔案 at {file_path_str}")
    # 您可以在此處添加退出或其他錯誤處理
else:
    # 2. 讀取檔案全部內容
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            full_content = f.read()
        
        print("--- 步驟 1: 檔案全部內容 ---")
        print(full_content)
        print("---------------------------")
        
        # 3. 讀取【防焊:】後面的文字
        # 定義我們要尋找的關鍵字
        keyword = "NO:"
        
        # 尋找關鍵字
        if keyword in full_content:
            # 找到關鍵字出現的位置 (第一個出現的位置)
            start_index = full_content.find(keyword)
            # 找到關鍵字後面文字的起始點
            value_start_index = start_index + len(keyword)
            
            # 從起始點開始，找到下一行 (換行符 \n) 的位置，作為結尾
            end_index = full_content.find('\n', value_start_index)
            
            # 如果找不到換行符，表示這是檔案的最後一行
            if end_index == -1:
                end_index = len(full_content)
            
            # 截取出需要的文字，並去除頭尾可能有的空格
            target_text = full_content[value_start_index:end_index].strip()
            
            print(f"--- 步驟 2: 【{keyword}】後面的文字 ---")
            print(target_text)
            print("---------------------------------")
            
        else:
            print(f"在檔案中找不到關鍵字：【{keyword}】")

    except Exception as e:
        print(f"讀取或處理檔案時發生錯誤: {e}")