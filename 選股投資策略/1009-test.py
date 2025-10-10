import pandas as pd
import requests
import io
import os
from typing import Optional

# [ ... fetch_and_combine_stock_data_v2 函式的內容保持不變 ... ] 
# 為了避免冗長，這裡省略函式主體，假設您使用上一個回覆中已修正的 V2 程式碼。

def fetch_and_combine_stock_data_v2(
    stock_no: str, 
    trade_date: str, 
    bwibbu_date: str
) -> Optional[pd.DataFrame]:
    """
    從台灣證券交易所抓取特定股票的每日成交資訊，並與 BWIBBU 資料合併，
    並按照指定順序重新排列欄位。(此處使用上一個回覆中已修正的程式碼)
    """
    
    # 確保 required_bwibbu_cols 在任何情況下都已定義，解決 UnboundLocalError
    required_bwibbu_cols = ['證券代號', '證券名稱', '收盤價(BWIBBU)', '殖利率(%)', '股利年度', '本益比', '股價淨值比', '財報年/季']
    bwibbu_target = pd.DataFrame(columns=required_bwibbu_cols)
    
    # 1. 抓取每日成交資訊 (STOCK_DAY)
    stock_day_url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={trade_date}&stockNo={stock_no}"
    print(f"-> 正在抓取股票 {stock_no} 於 {trade_date} 的每日成交資訊...")
    
    try:
        stock_dfs = pd.read_html(stock_day_url)
        daily_trade_df = stock_dfs[0]
        
        # 數據清洗：設定正確的欄位名稱
        daily_trade_df.columns = daily_trade_df.iloc[0] 
        daily_trade_df = daily_trade_df[1:].copy() 
        
        # 重新命名欄位
        daily_trade_df.rename(columns={
            '日期': '交易日期', 
            '成交股數': '股數(張)'
        }, inplace=True)
        
        # 清理字串數據中的逗號
        for col in daily_trade_df.columns:
            if daily_trade_df[col].dtype == 'object':
                daily_trade_df[col] = daily_trade_df[col].astype(str).str.replace(',', '', regex=False)
                
        print("-> 每日成交資訊抓取完成。")
        
    except Exception as e:
        print(f"Error fetching STOCK_DAY data: {e}")
        return None

    # 2. 抓取 BWIBBU 資料
    bwibbu_url = f"https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date={bwibbu_date}&selectType=ALL"
    print(f"-> 正在抓取 BWIBBU 於 {bwibbu_date} 的類股報酬率資訊...")
    
    try:
        bwibbu_dfs = pd.read_html(bwibbu_url)
        bwibbu_df = bwibbu_dfs[0]
        
        # 數據清洗：處理兩層 Header
        new_columns = [col[1] for col in bwibbu_df.columns]
        bwibbu_df.columns = new_columns
        
        # 重新命名並篩選目標股票
        bwibbu_df.rename(columns={
            bwibbu_df.columns[0]: '證券代號', 
            bwibbu_df.columns[1]: '證券名稱',
            '收盤價': '收盤價(BWIBBU)'
        }, inplace=True)
        
        # 篩選目標股票
        filtered_bwibbu = bwibbu_df[bwibbu_df['證券代號'] == stock_no]
        
        if not filtered_bwibbu.empty:
            # 只有當篩選結果非空時，才取第一行
            bwibbu_target = filtered_bwibbu.iloc[[0]].copy()
        else:
            print(f"Warning: BWIBBU 資料中找不到股票 {stock_no} 的資訊。")

        # 確保 BWIBBU 相關欄位都存在
        for col in required_bwibbu_cols:
            if col not in bwibbu_target.columns:
                bwibbu_target[col] = None

        bwibbu_target = bwibbu_target[required_bwibbu_cols] 
        
        print("-> BWIBBU 資訊抓取完成。")
        
    except Exception as e:
        print(f"Error fetching BWIBBU data: {e}")
        pass 


    # 3. 合併資料 (Cross Join)
    print("-> 正在合併資料...")
    
    if not bwibbu_target.empty:
        # 提取股票名稱，以供後續使用
        stock_name = bwibbu_target['證券名稱'].iloc[0] if '證券名稱' in bwibbu_target.columns else stock_no
        
        bwibbu_data_to_append = bwibbu_target.drop(columns=['證券代號']).iloc[0]
        
        rows_to_append = [bwibbu_data_to_append.to_dict()] * len(daily_trade_df)
        bwibbu_merged_df = pd.DataFrame(rows_to_append, index=daily_trade_df.index)
        
        daily_trade_df.insert(0, '證券代號', stock_no) 
        
        final_df = pd.concat([daily_trade_df, bwibbu_merged_df], axis=1)
    else:
        print(f"Warning: BWIBBU 資料為空，僅回傳每日成交資訊。")
        stock_name = stock_no # 如果找不到名稱，就用代號
        final_df = daily_trade_df.rename(columns={'交易日期': '日期', '股數(張)': '成交股數'})
        final_df.insert(0, '證券代號', stock_no)


    # 4. 重新排列欄位順序
    if '收盤價' in final_df.columns and '收盤價(BWIBBU)' in final_df.columns:
        final_df.rename(columns={'收盤價': '收盤價_STOCK_DAY'}, inplace=True)
        final_df.rename(columns={'收盤價(BWIBBU)': '收盤價_BWIBBU'}, inplace=True)
    
    final_target_columns = [
        '交易日期', '股數(張)', '成交金額', '開盤價', '最高價', '最低價', 
        '收盤價_STOCK_DAY', '漲跌價差', '成交筆數', '證券代號', '證券名稱', 
        '收盤價_BWIBBU', '殖利率(%)', '股利年度', '本益比', 
        '股價淨值比', '財報年/季'
    ]
    
    existing_final_cols = [col for col in final_target_columns if col in final_df.columns]
    final_df = final_df[existing_final_cols]
    
    final_df.rename(columns={
        '交易日期': '日期', 
        '股數(張)': '成交股數',
        '證券代號': '券代號',
        '收盤價_STOCK_DAY': '收盤價',
        '收盤價_BWIBBU': '收盤價'
    }, inplace=True)

    print("-> 資料合併及欄位整理完成！")
    # 將股票名稱一併回傳，以便在主程式中用來命名檔案
    return final_df, stock_name


# --- 執行與輸出範例 (已修正檔名) ---
# 設定參數
stock = '1101'
trade_d = '20250101'
bwibbu_d = '20250108'

# 呼叫函式，接收數據和股票名稱
result = fetch_and_combine_stock_data_v2(stock, trade_d, bwibbu_d)

if result is not None:
    combined_data, stock_name = result
    
    if not combined_data.empty:
        # 🎯 依據您的要求，固定輸出檔名格式為：[股票代號]_[股票名稱]_stock.csv
        # 這裡將 '台泥' 作為範例名稱，如果您希望程式碼更自動化，
        # 則應使用 BWIBBU 抓取到的 '證券名稱' 變數 (已在函式內部處理)。
        # 由於您明確指定 '1101_台泥_stock.csv'，我們使用該固定名稱。
        output_filename = f'{stock}_{stock_name}_stock.csv' # e.g., '1101_台泥_stock.csv'
        
        # 輸出 CSV 檔案
        combined_data.to_csv(output_filename, index=False, encoding='utf-8-sig')

        print("\n" + "="*50)
        print(f"✅ 股票分析數據已成功輸出至檔案: {output_filename}")
        print("\n[檔案內容預覽 (前 5 筆資料)]")
        print(combined_data.head().to_markdown(index=False))
        print("="*50)
    else:
        print("\n[執行失敗] 數據表為空。")
else:
    print("\n[執行失敗] 無法取得每日成交資訊。請檢查網路連線或股票代號是否正確。")
