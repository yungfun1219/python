import pandas as pd
import os
from dateutil.rrule import rrule, DAILY, MO, TU, WE, TH, FR

# --- 1. 廣域設定參數 ---
base_directory = r'D:\Python_repo\python\Jason_Stock_Analyzer\datas\twse_holidays'
year_holidays = [2021, 2022, 2023, 2024, 2025, 2026] # 4. 所有年度
INPUT_FILE_TEMPLATE = 'twse_holidays_{}_OK.csv'
HOLIDAY_OUTPUT_FILE_NAME = 'twse_holidays_{}~{}.csv' # 5. 休市日輸出檔名樣板
TRADING_DAY_OUTPUT_FILE_NAME = 'trading_day_{}-{}.csv' # 7. 交易日輸出檔名樣板
DATE_FORMAT = '%Y/%#m/%#d' # 統一日期格式 (例如 2021/1/1)


def process_date_and_day(date_str, current_year):
    """
    輔助函式：將 '月日(星期)' 格式轉換為 'YYYY/M/D' 和 '星期X'。
    """
    if pd.isna(date_str):
        return None, None
    
    # 找到括號內的星期縮寫，例如 '(四)'
    day_abbr = date_str[-3:] if date_str.endswith(')') else ''
    
    # 提取 '月日' 部分，例如 '1月1日'
    date_part = date_str[:-3] if date_str.endswith(')') else date_str
    
    # 轉換為目標日期格式 'YYYY/月/日'
    new_date = f'{current_year}/' + date_part.replace('月', '/').replace('日', '')
    
    # 轉換星期縮寫為中文全稱
    day_mapping = {
        '(一)': '星期一', '(二)': '星期二', '(三)': '星期三', 
        '(四)': '星期四', '(五)': '星期五', '(六)': '星期六', '(日)': '星期日'
    }
    new_day = day_mapping.get(day_abbr, '未知')

    return new_date, new_day


def process_and_consolidate_holidays(years, directory):
    """
    4 & 5. 處理所有年度的休市日檔案，並彙整儲存。
    """
    print("🚀 開始處理多年度休市日資料並彙整...")
    all_dfs = []
    
    for year in years:
        input_file_name = INPUT_FILE_TEMPLATE.format(year)
        input_file_path = os.path.join(directory, input_file_name)
        
        print(f"\n--- 處理年份: {year} ({input_file_name}) ---")

        try:
            # 1. CSV 檔案解碼使用 'big5'
            df = pd.read_csv(input_file_path, encoding='big5') 
            
            if '日期' not in df.columns:
                print("❌ 錯誤：CSV 檔案中找不到名為 [日期] 的欄位。跳過。")
                continue

            # 取得 '日期' 欄位索引 (假設在 A 欄，索引 0)
            date_col_index = df.columns.get_loc('日期')
            
            # 2. 不使用 lambda 指令，直接使用 apply 呼叫輔助函式
            temp_df = df['日期'].apply(lambda x: process_date_and_day(x, year)).apply(pd.Series)
            
            # 覆蓋原 [日期] 欄位 (A欄)
            df['日期'] = temp_df[0]
            
            # 3. 將增加的欄位 [星期] 插入到索引 1 (B欄)
            df.insert(loc=date_col_index + 1, column='星期', value=temp_df[1])
            
            print(f"✨ {year} 年資料處理完成。")
            all_dfs.append(df)

        except FileNotFoundError:
            print(f"❌ 錯誤：找不到 {year} 年的檔案: {input_file_path}")
        except Exception as e:
            print(f"❌ 處理 {year} 年資料時發生錯誤: {e}")

    # 5. 彙整與儲存
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # 5. 檔案名稱自動修改儲存
        min_year = min(years)
        max_year = max(years)
        final_output_file_name = HOLIDAY_OUTPUT_FILE_NAME.format(min_year, max_year)
        final_output_file_path = os.path.join(directory, final_output_file_name)
        
        # 儲存時使用 'big5'
        final_df.to_csv(final_output_file_path, index=False, encoding='big5') 
        
        print(f"\n✅ 休市日檔案已成功彙整並儲存至:")
        print(f"    👉 {final_output_file_path}")
        return final_output_file_path
    
    return None


def create_trading_days_list(years, directory, holiday_file_path):
    """
    6 & 7. 生成交易日清單並儲存。
    """
    print("\n\n--- 步驟 6: 計算交易日清單 ---")
    start_year = min(years)
    end_year = max(years)
    
    # --- 6.1 生成所有上班日 (週一至週五) ---
    start_date = pd.to_datetime(f'{start_year}-01-01')
    end_date = pd.to_datetime(f'{end_year}-12-31')
    
    weekdays = rrule(
        DAILY, 
        dtstart=start_date.date(), 
        until=end_date.date(),
        byweekday=(MO, TU, WE, TH, FR)
    )
    all_weekdays_dt = pd.to_datetime(list(weekdays))

    weekday_df = pd.DataFrame({
        '日期': all_weekdays_dt.strftime(DATE_FORMAT), 
        '星期': all_weekdays_dt.strftime('%w').astype(int).map({
            1: '星期一', 2: '星期二', 3: '星期三', 4: '星期四', 5: '星期五'
        })
    })
    
    all_weekdays_set = set(weekday_df['日期'])
    print(f"✅ 已生成所有平日共 {len(all_weekdays_set)} 筆。")

    # --- 6.2 讀取並標準化休市日 ---
    try:
        # 1. CSV 檔案解碼使用 'big5'
        holidays_df = pd.read_csv(holiday_file_path, encoding='big5')
        
        # 確保日期格式與平日清單一致，以便準確比對
        holidays_df['日期'] = pd.to_datetime(holidays_df['日期']).dt.strftime(DATE_FORMAT)
        holidays_set = set(holidays_df['日期'].tolist())
        print(f"✅ 已讀取休市日檔案，共 {len(holidays_set)} 筆。")

    except Exception as e:
        print(f"❌ 讀取休市日檔案時發生錯誤: {e}")
        return

    # --- 6.3 排除休市日，得到交易日 ---
    trading_days_set = all_weekdays_set - holidays_set
    
    # 過濾平日 DataFrame，只留下交易日
    trading_days_df = weekday_df[weekday_df['日期'].isin(trading_days_set)]
    
    print(f"🎉 最終交易日清單生成完成，共 {len(trading_days_df)} 筆交易日。")

    # --- 7. 儲存交易日清單 ---
    trading_day_file_name = TRADING_DAY_OUTPUT_FILE_NAME.format(start_year, end_year)
    trading_day_file_path = os.path.join(directory, trading_day_file_name)
    
    # 儲存時使用 'big5'
    trading_days_df.to_csv(trading_day_file_path, index=False, encoding='big5')
    
    print(f"\n✅ 交易日清單已成功儲存至:")
    print(f"    👉 {trading_day_file_path}")


# --- 主程式執行區塊 ---
if __name__ == '__main__':
    # 步驟 1: 處理並彙整所有年度休市日
    consolidated_path = process_and_consolidate_holidays(year_holidays, base_directory)
    
    if consolidated_path:
        # 步驟 2: 計算並儲存交易日清單
        create_trading_days_list(year_holidays, base_directory, consolidated_path)
    else:
        print("\n無法生成休市日彙整檔案，交易日計算中止。")