import pandas as pd
import os
from dateutil.rrule import rrule, DAILY, MO, TU, WE, TH, FR

# --- 設定處理參數 ---
base_directory = r'D:\Python_repo\python\Jason_Stock_Analyzer\datas\twse_holidays'
holiday_file_name = 'twse_holidays_2021~2026.csv'
holiday_file_path = os.path.join(base_directory, holiday_file_name)
target_years = [2021, 2022, 2023, 2024, 2025, 2026]

# 最終輸出檔案的名稱和路徑
trading_day_file_name = 'trading_day_2021-2026.csv'
trading_day_file_path = os.path.join(base_directory, trading_day_file_name)

# 定義統一的日期格式 (用於集合比對)
DATE_FORMAT = '%Y/%#m/%#d' # 例如：2021/1/1

def get_trading_days(start_year, end_year, holiday_csv_path):
    """
    生成指定年度範圍內的所有交易日（排除週六、週日和特定休市日）。
    """
    print("--- 步驟 1: 生成所有平日清單 (週一至週五) ---")
    
    start_date = pd.to_datetime(f'{start_year}-01-01')
    end_date = pd.to_datetime(f'{end_year}-12-31')
    
    # 使用 dateutil.rrule 生成所有平日日期
    weekdays = rrule(
        DAILY, 
        dtstart=start_date.date(), 
        until=end_date.date(),
        byweekday=(MO, TU, WE, TH, FR)
    )
    
    all_weekdays_dt = pd.to_datetime(list(weekdays))

    # 建立所有平日的 DataFrame，並格式化日期
    weekday_df = pd.DataFrame({
        '日期': all_weekdays_dt.strftime(DATE_FORMAT), 
        '星期': all_weekdays_dt.strftime('%w').astype(int).map({
            1: '星期一', 2: '星期二', 3: '星期三', 4: '星期四', 5: '星期五'
        })
    })
    
    all_weekdays_set = set(weekday_df['日期'])
    print(f"✅ 已生成 {start_year}~{end_year} 所有平日共 {len(all_weekdays_set)} 筆日期。")

    print("\n--- 步驟 2: 讀取休市日清單並標準化日期 ---")
    
    try:
        # **使用 'big5' 讀取休市日檔案**
        holidays_df = pd.read_csv(holiday_csv_path, encoding='big5')
        
        # 確保休市日清單的日期格式與平日清單完全一致
        holidays_df['日期'] = pd.to_datetime(holidays_df['日期']).dt.strftime(DATE_FORMAT)
        
        holidays_set = set(holidays_df['日期'].tolist())
        print(f"✅ 已讀取休市日檔案，共 {len(holidays_set)} 筆休市日期。")

    except FileNotFoundError:
        print(f"❌ 錯誤：找不到休市日檔案。請檢查路徑: {holiday_csv_path}")
        return []
    except Exception as e:
        print(f"❌ 讀取休市日檔案時發生錯誤: {e}")
        return []

    print("\n--- 步驟 3: 比對並移除休市日 (計算交易日) ---")
    
    # 核心排除步驟：從所有平日集合中，減去休市日集合
    trading_days_set = all_weekdays_set - holidays_set
    
    # 過濾平日 DataFrame，只留下交易日
    trading_days_df = weekday_df[weekday_df['日期'].isin(trading_days_set)]
    
    # 將 DataFrame 轉換為清單格式
    trading_days_list = trading_days_df[['日期', '星期']].values.tolist()
    
    print(f"🎉 最終交易日清單生成完成，共 {len(trading_days_list)} 筆交易日。")
    
    return trading_days_list

# --- 執行函式、儲存檔案 ---
if __name__ == '__main__':
    
    print("=" * 40)
    # 呼叫函式
    all_trading_days = get_trading_days(
        start_year=min(target_years),
        end_year=max(target_years),
        holiday_csv_path=holiday_file_path
    )
    
    if all_trading_days:
        print("\n--- 步驟 4: 儲存交易日清單為 CSV 檔案 ---")
        
        try:
            # 將清單轉換為 DataFrame
            df_trading_days = pd.DataFrame(all_trading_days, columns=['日期', '星期'])
            
            # **儲存為 trading_day_2021-2026.csv，使用 'big5' 編碼**
            df_trading_days.to_csv(trading_day_file_path, index=False, encoding='big5')
            
            print(f"✅ 交易日清單已成功儲存至:")
            print(f"    👉 {trading_day_file_path}")
            
        except Exception as e:
            print(f"❌ 儲存交易日檔案時發生錯誤: {e}")
            
    else:
        print("⚠️ 由於無法獲取交易日資料，未生成交易日 CSV 檔案。")
        
    print("\n🎉 所有作業完成。")