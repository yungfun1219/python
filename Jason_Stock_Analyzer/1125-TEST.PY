import pandas as pd
import os
import re
from typing import List, Dict, Set, Optional, Tuple

# ----------------------------------------------------------------------
# 輔助函式: 讀取並過濾單日數據
# ----------------------------------------------------------------------

def _load_and_filter_single_day(
    file_path: str,
    top_n: int,
    volume_column: str,
    code_column: str
) -> Optional[pd.DataFrame]:
    """
    讀取單日 T86 CSV 檔案，篩選上市公司，並依買超量排序。

    Args:
        file_path: 檔案路徑。
        top_n: 篩選數量 (用於檢查買超量)。
        volume_column: 買賣超股數欄位名稱 ("三大法人買賣超股數")。
        code_column: 證券代號欄位名稱 ("證券代號")。

    Returns:
        包含代號、名稱、股數的 DataFrame (已排序)，或 None。
    """
    REQUIRED_COLUMNS = [code_column, "證券名稱", volume_column]

    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 1. 確保關鍵欄位存在
        if not all(col in df.columns for col in REQUIRED_COLUMNS):
            print(f"    ❌ 欄位缺失。需要: {REQUIRED_COLUMNS}")
            return None
        
        # 2. 篩選：只保留上市公司 (假設為 4 位純數字代號)
        df = df[
            df[code_column].astype(str).str.match(r'^\d{4}$')
        ].copy()

        # 3. 數據清理：確保買賣超股數是數字，並處理買超 (> 0)
        df[volume_column] = pd.to_numeric(
            df[volume_column].astype(str).str.replace(',', '').str.strip(),
            errors='coerce'
        )
        df.dropna(subset=[volume_column], inplace=True)
        
        # 4. 篩選：只保留買超 (> 0) 的股票
        df = df[df[volume_column] > 0].copy()
        
        # 5. 依買超量由大到小排序
        df.sort_values(by=volume_column, ascending=False, inplace=True)
        
        # 6. 整理並重命名欄位，取 Top N 的代號和名稱
        df_result = df[[code_column, "證券名稱", volume_column]].rename(
            columns={
                code_column: '代號', 
                "證券名稱": '名稱', 
                volume_column: '股數'
            }
        ).head(top_n)
        
        return df_result
        
    except FileNotFoundError:
        print(f"    ❌ 檔案未找到: {file_path}")
        return None
    except Exception as e:
        print(f"    ❌ 載入數據時發生錯誤: {e}")
        return None


# ----------------------------------------------------------------------
# 主分析函式
# ----------------------------------------------------------------------

def analyze_top_stocks_trend(
    file_paths: List[str],
    top_n: int = 30, 
    n_days_lookback: int = 5, 
    volume_column: str = "三大法人買賣超股數", 
    code_column: str = "證券代號"
) -> Optional[str]:
    """
    分析最新一日三大法人買超 Top N 股票在過去 N 天的回溯趨勢。
    輸出結果不包含名次，並依 代號, 證券名稱, 回溯趨勢, 買超張數 排序。
    回溯趨勢的日期標籤和標記皆以「最舊日到最新日」的順序排列。

    Args:
        file_paths (List[str]): 依序為 [最新日, 前一日, ..., 前第 N 日] 的檔案路徑列表。
        top_n (int): 基準日要篩選出的前 N 名數量 (預設 30)。
        n_days_lookback (int): 要回溯的交易日天數 (預設 5)。
        volume_column (str): 買賣超股數欄位名稱。
        code_column (str): 證券代號欄位名稱。

    Returns:
        Optional[str]: 格式化輸出趨勢結果，或錯誤訊息。
    """
    
    # 確保有足夠的檔案進行基準日 + 回溯日分析
    required_files = n_days_lookback + 1
    if len(file_paths) < required_files:
        return f"⚠️ 錯誤：至少需要 {required_files} 個檔案 (基準日 + {n_days_lookback} 個回溯日)。目前只有 {len(file_paths)} 個。"

    # --- 1. 處理所有交易日數據 ---
    all_day_data: Dict[int, Set[str]] = {}
    # day_labels 的順序: [基準日, 回溯日1, 回溯日2, ...]
    day_labels: List[str] = [] 
    df_base_day: Optional[pd.DataFrame] = None 

    print(f"🔍 開始處理 {required_files} 天數據...")
    
    for i, path in enumerate(file_paths[:required_files]):
        # 載入、過濾並排序單日數據
        df_day = _load_and_filter_single_day(path, top_n, volume_column, code_column)
        
        # 提取檔案名稱中的日期作為標籤
        try:
            # 假設檔案名稱包含日期，例如 "20251110_institutional.csv"
            date_label = os.path.basename(path).split('_')[0][:8]
        except:
            date_label = f"Day-{i}"
            
        if df_day is None or df_day.empty:
            print(f"⚠️ {date_label} 數據載入或篩選失敗，將略過此日。")
            # 即使失敗，也要確保在字典中存在空集，以防止後續 KeyErorr
            all_day_data[i] = set() 
        else:
            # 將該日期的 Top N 股票代號存儲為集合 (Set)
            all_day_data[i] = set(df_day['代號'].astype(str).tolist())
            print(f"✅ {date_label} 成功篩選出 {len(all_day_data[i])} 檔股票。")

        day_labels.append(date_label)
        
        # 基準日 (最新日) 的數據需要單獨保存 (只取 Top N)
        if i == 0:
            df_base_day = df_day.head(top_n) 
    
    if df_base_day is None or df_base_day.empty:
        return "❌ 基準日 (最新日) 數據無效或為空，無法進行趨勢分析。"
        
    # --- 2. 獲取基準日 Top N 股票代號 (此為分析的主體) ---
    base_stocks = df_base_day[['代號', '名稱', '股數']].copy()
    
    # --- 3. 執行回溯趨勢分析 ---
    
    # trend_header_parts 的順序是 [回溯日1, 回溯日2, ...]
    trend_header_parts: List[str] = []
    trend_marker_series: List[pd.Series] = []
    
    # i=0 是基準日，從 i=1 開始是回溯日 (最新回溯日到最舊回溯日)
    for i in range(1, required_files):
        # 回溯日期的標籤
        day_tag = day_labels[i].replace("/", "")[-4:] # 取日期後四碼 (如 1110)
        trend_header_parts.append(day_tag)
        
        # 取得該回溯日期的 Top N 股票代號集合
        past_top_n_codes = all_day_data.get(i, set())
        
        # 建立趨勢標記 Series
        presence_markers = [
            "🔴" if code in past_top_n_codes else "⚪️"
            for code in base_stocks['代號']
        ]
        
        # 將標記 Series 存入列表
        trend_marker_series.append(pd.Series(presence_markers, index=base_stocks.index))
        
    # *** 關鍵修改：將列表反轉，使順序變為「最舊日到最新日」 ***
    # trend_header_parts: [最舊回溯日, ..., 最新回溯日]
    # trend_marker_series: [最舊回溯日標記, ..., 最新回溯日標記]
    trend_header_parts.reverse()
    trend_marker_series.reverse()
    
    # 建立最終的回溯趨勢標頭 (最舊日到最新日)
    trend_header = " ".join(trend_header_parts)
    
    # 串接所有的趨勢標記 Series (最舊日到最新日)
    # 我們使用一個空格作為標記的分隔
    if trend_marker_series:
        # 初始趨勢欄位 (最舊日)
        base_stocks['趨勢'] = trend_marker_series[0].astype(str)
        for series in trend_marker_series[1:]:
            # 每次加上一個空格和下一個日期的標記
            base_stocks['趨勢'] += " " + series.astype(str)
    else:
        base_stocks['趨勢'] = pd.Series("", index=base_stocks.index) # 無回溯日
    
    # --- 4. 格式化輸出結果 ---
    
    # 將股數轉換為張數並格式化
    volume_col_display_name = '買超張數'
    base_stocks[volume_col_display_name] = base_stocks['股數'].apply(lambda x: f"{int(x / 1000):,}")
    
    # 依照要求排序: 代號, 證券名稱, 回溯趨勢, 買超張數
    # 註: 此處無法依「回溯趨勢」和「買超張數」排序，因為它們是字串。
    #     為了保持與基準日買超量 Top N 的一致性，我們主要依基準日買超量隱式排序。
    
    # 建立表格標頭
    output_lines = [
        f"\n*************************************",
        f"   📈 三大法人買超Top{top_n} {day_labels[0]} 趨勢分析",
        f"*************************************",
        f" 代號 | 名稱 | {trend_header} | 張數",
        f"-------------------------------------"
    ]
    
    # 建立表格內容
    for _, row in base_stocks.iterrows():
        code_str = str(row['代號']).center(4) 
        # 中文名稱處理 (假設佔用 4 個標準字寬，使用 ljust 確保對齊)
        name_str = row['名稱'].ljust(4, '　')
        volume_str = row[volume_col_display_name].rjust(6) # 右對齊數字
        trend_str = row['趨勢']
        
        # 輸出順序: 代號 | 證券名稱 | 回溯趨勢 | 買超張數
        output_lines.append(
            f"{code_str} | {name_str[:4]} | {trend_str} | {volume_str}"
        )
        
    output_lines.append("=" * 37)
    output_lines.append(f"🔴: 該日出現在 Top {top_n} 名單中")
    output_lines.append(f"⚪️: 該日未出現在 Top {top_n} 名單中")
    
    return "\n".join(output_lines)

# ----------------------------------------------------------------------
# 執行範例 (請替換為您實際的檔案路徑)
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # 範例檔案路徑 (假設這些檔案已存在並包含 T86 數據)
    # 順序: [最新日, 前一日, 前兩日, ...]
    
    # ⚠️ 僅供測試。請將這些路徑替換為您的實際檔案路徑。
    # 假設 CODE_DIR 是專案根目錄
    CODE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(CODE_DIR, "datas", "raw", "11_T86")
    
    # 這裡使用虛擬日期來模擬檔案清單
    try:
        from datetime import date, timedelta
        file_dates = [date(2025, 11, 21) - timedelta(days=i) for i in range(6)]
        
        # 假設您的 T86 檔案命名格式是 {日期}_T86_InstitutionalTrades.csv
        mock_file_paths = [
            os.path.join(DATA_DIR, f"{d.strftime('%Y%m%d')}_T86_InstitutionalTrades.csv")
            for d in file_dates
        ]
        
        # --- 執行分析 ---
        result = analyze_top_stocks_trend(
            file_paths=mock_file_paths,
            top_n=10, 
            n_days_lookback=5 # 回溯 5 天 (共需 6 個檔案)
        )
        
        if result:
            print(result)
            
    except Exception as e:
        print(f"程式執行失敗: {e}")
        print("請確認 'mock_file_paths' 中的檔案路徑是否正確，以及檔案是否存在。")