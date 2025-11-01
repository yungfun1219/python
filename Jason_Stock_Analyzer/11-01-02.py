import pandas as pd
from datetime import datetime
import pathlib

def get_past_dates_in_yyyymmdd(file_path, date_column_name='日期'):
    """
    讀取 CSV 檔案，篩選出在今天或今天之前的所有日期，並以 YYYYMMDD 字串格式返回。

    Args:
        file_path (str): CSV 檔案的路徑。
        date_column_name (str): CSV 中包含日期的欄位名稱。預設為 'Date'。

    Returns:
        list: 包含所有過去日期的 YYYYMMDD 格式字串列表，如果出錯則返回空列表。
    """
    try:
        # 1. 讀取 CSV 檔案
        df = pd.read_csv(file_path)

        # 2. 確保日期欄位是 datetime 格式
        # errors='coerce' 會將無法解析的值設為 NaT
        df[date_column_name] = pd.to_datetime(df[date_column_name], errors='coerce')

        # 3. 獲取今天的日期 (只取年月日部分)
        # 今天的日期為 2025-11-01
        today = pd.to_datetime(datetime.now().date()) 
        
        # 4. 篩選出今天之前 (即 <= 今天) 的日期資料
        # 篩選條件是：日期欄位值 <= 今天的日期
        past_dates_df = df[df[date_column_name] <= today]

        # 5. 移除日期為 NaT 的列
        past_dates_df = past_dates_df.dropna(subset=[date_column_name])
        
        # 6. 排序 (可選，通常日期資料按時間順序排列較好)
        past_dates_df = past_dates_df.sort_values(by=date_column_name)
        
        # 7. **【關鍵】格式化並返回日期列表**
        # 使用 .dt.strftime('%Y%m%d') 將 datetime 物件轉換為 YYYYMMDD 格式的字串
        yyyymmdd_list = past_dates_df[date_column_name].dt.strftime('%Y%m%d').tolist()
        
        return yyyymmdd_list

    except FileNotFoundError:
        print(f"錯誤：找不到檔案在路徑：{file_path}")
        return []
    except KeyError:
        print(f"錯誤：CSV 檔案中找不到名為 '{date_column_name}' 的日期欄位。")
        return []
    except Exception as e:
        print(f"發生其他錯誤：{e}")
        return []

# --- 主要執行部分 ---
file_path = pathlib.Path(__file__).resolve().parent / "datas" / "processed" / "get_holidays" / "trading_day_2021-2025.csv"
# 假設您的日期欄位名稱就是 'Date'
past_dates_yyyymmdd = get_past_dates_in_yyyymmdd(file_path, date_column_name='日期')

if past_dates_yyyymmdd:
    print(f"🎉 成功取得在 {datetime.now().date()} 或之前的所有日期 (YYYYMMDD 格式)：")
    # print("--------------------------------------------------")
    # # 輸出結果列表
    # print(past_dates_yyyymmdd)
    # print("--------------------------------------------------")
    print(f"總共篩選出 {len(past_dates_yyyymmdd)} 筆日期。")
else:
    print("沒有找到符合條件的日期資料，請檢查檔案路徑和內容。")