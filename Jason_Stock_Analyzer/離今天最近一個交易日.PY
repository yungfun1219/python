import pandas as pd
from datetime import datetime
import pathlib
from typing import Optional

# --- 檔案與路徑設定 ---

# 假設您的程式碼路徑在 D:\Python_repo\python\Jason_Stock_Analyzer
BASE_DIR = pathlib.Path(r"D:\Python_repo\python\Jason_Stock_Analyzer")

# 輸入檔案路徑：使用您生成的多年份交易日檔案
TRADING_DAY_PATH = BASE_DIR / "datas" / "processed" / "get_holidays" / "trading_day_2021-2025.csv"

def find_latest_past_working_day(file_path: pathlib.Path) -> Optional[str]:
    """
    從交易日清單中，找出離今天最近的一個過去的工作日。

    Args:
        file_path: 交易日清單 CSV 檔案的路徑。

    Returns:
        最近的一個工作日 (YYYYMMDD 格式字串)，如果找不到則返回 None。
    """
    print(f"--- 正在查找 {file_path.name} 中的最近工作日 ---")

    # 1. 讀取交易日清單
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 確保日期欄位名稱正確 (假設是 'Trading_day')
        if 'Trading_day' not in df.columns:
            print("❌ 錯誤：CSV 檔案中找不到 'Trading_day' 欄位。")
            return None
            
        # 將日期轉換為字串並去除空白
        all_trading_days = df['Trading_day'].astype(str).str.strip().tolist()
        
    except FileNotFoundError:
        print(f"❌ 錯誤：找不到交易日檔案：{file_path}")
        return None
    except Exception as e:
        print(f"❌ 錯誤：讀取交易日檔案時發生問題：{e}")
        return None

    # 2. 確定今天的日期 (YYYYMMDD 格式字串)
    today_str = "20250201"#datetime.now().strftime('%Y%m%d')
    print(f"【今天日期】: {today_str}")

    # 3. 篩選出所有在今天之前的交易日
    # 由於日期都是 YYYYMMDD 格式的字串，可以直接用字串比較
    past_working_days = [
        date_str 
        for date_str in all_trading_days 
        if date_str < today_str
    ]

    # 4. 找出最大的日期（即離今天最近的日期）
    if not past_working_days:
        print("⚠️ 警告：在清單中找不到任何過去的工作日。")
        return None
    
    # 對過去的工作日進行排序，取最後一個 (最大的那個)
    latest_working_day = max(past_working_days)
    
    return latest_working_day


# --- 執行範例 ---
if __name__ == "__main__":
    
    # 檢查檔案路徑是否存在
    if not TRADING_DAY_PATH.exists():
        print(f"\n致命錯誤：請先執行上一個程式碼，生成交易日清單於：\n{TRADING_DAY_PATH}")
    else:
        latest_day = find_latest_past_working_day(TRADING_DAY_PATH)
        
        print("\n=============================================")
        if latest_day:
            print(f"🟢 離今天最近的一個過去工作日是：【{latest_day}】")
        else:
            print("🔴 無法確定最近的工作日。")
        print("=============================================")