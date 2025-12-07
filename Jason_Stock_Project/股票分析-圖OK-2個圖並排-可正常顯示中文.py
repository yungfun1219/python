import pandas as pd
from pathlib import Path
import re
from datetime import datetime, timedelta
import os
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib as mpl
import shutil
from matplotlib import font_manager 
from matplotlib.lines import Line2D 

# -----------------------------
# 【設定區】
# -----------------------------
# 股票列表
STOCK_LIST = [
    {"code": "2344", "name": "華邦電"},
    {"code": "1802", "name": "台玻"},
]

# 路徑設定
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
BASE_RAW_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"

# 新的固定輸出目錄
NEW_OUTPUT_DIR_STR = r"D:\Python_repo\python\Jason_Stock_Project\datas\processed\stock_all"
OUTPUT_DIR = Path(NEW_OUTPUT_DIR_STR)

# 字體設定：請將 'NotoSansTC-VariableFont_wght.ttf' 檔案放在專案根目錄
FONT_FILENAME = "NotoSansTC-VariableFont_wght.ttf" 
FONT_PATH = BASE_DIR / FONT_FILENAME 

# MA 線的顏色定義
MA_COLORS = {
    'MA5': 'purple', 
    'MA10': 'darkgreen', 
    'MA20': 'gold'
}

# 資料欄位定義
ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950']
PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
ALL_REQUIRED_COLS = ['日期'] + PRICE_COLS

# -----------------------------
# 【資料處理函式】
# -----------------------------

def convert_roc_to_gregorian(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None
    s = date_str.strip()
    trans_table = str.maketrans('，。／－：．', ',./-:.')
    s = s.translate(trans_table)
    s = s.replace('年', '/').replace('月', '/').replace('日', '')
    m = re.search(r'(\d{2,4})\D+(\d{1,2})\D+(\d{1,2})', s)
    if not m:
        return None
    year_str = m.group(1)
    mon_str = m.group(2)
    day_str = m.group(3)
    try:
        year = int(year_str)
        month = int(mon_str)
        day = int(day_str)
    except ValueError:
        return None
    
    if year < 1912:
        greg_year = year + 1911
    else:
        greg_year = year
        
    try:
        d = datetime(greg_year, month, day)
        return f"{d.year}/{d.month:02d}/{d.day:02d}"
    except ValueError:
        return None

def try_read_csv(filepath, encodings):
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, encoding=enc, header=0)
            return df, enc
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"❌ {filepath.name} 使用 {enc} 讀取時發生非編碼錯誤: {e}")
            return None, None
    print(f"⚠️ {filepath.name} 無法用指定編碼讀取。")
    return None, None

def clean_dataframe(df):
    df.columns = df.columns.str.strip()
    for col in ALL_REQUIRED_COLS:
        if col not in df.columns:
            raise KeyError(f"資料缺少欄位: {col}")
    
    df['日期'] = df['日期'].astype(str).apply(convert_roc_to_gregorian)
    df = df.dropna(subset=['日期']).copy()
    
    df['日期'] = pd.to_datetime(df['日期'], format='%Y/%m/%d', errors='coerce')
    df = df.dropna(subset=['日期']).copy()
    
    for col in PRICE_COLS:
        s = (
            df[col]
            .astype(str)
            .str.replace('，', ',', regex=False)
            .str.replace('－', '-', regex=False)
            .str.replace(r'[^0-9\.\-]', '', regex=True)
        )
        df[col] = pd.to_numeric(s.replace({'': pd.NA}), errors='coerce')
        
    df = df.dropna(subset=ALL_REQUIRED_COLS).copy()
    return df[ALL_REQUIRED_COLS].copy()

def load_and_process_data(stock_code, stock_name, output_dir):
    """載入、清理、計算 MA 並準備繪圖數據，保留所有原始欄位並新增 MA 欄位"""
    
    # 假設這兩個變數已在主程式中定義
    # BASE_RAW_DIR = Path('您的原始資料夾路徑')
    # ENCODINGS_TO_TRY = ['utf-8', 'big5'] 
    # PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
    
    input_dir_stock = BASE_RAW_DIR / stock_code
    
    print(f"\n--- 載入與計算 {stock_code} ({stock_name}) ---")

    if not input_dir_stock.exists():
        print(f"❌ 指定的資料夾不存在：{input_dir_stock}")
        return None
    
    all_data_frames = []
    file_list = sorted([p for p in input_dir_stock.iterdir() if p.suffix.lower() == '.csv'])
    if not file_list:
        print("⚠️ 資料夾內沒有 csv 檔案。")
        return None

    for filepath in file_list:
        # 假設 try_read_csv 和 clean_dataframe 函式在您的環境中可用
        df_raw, _ = try_read_csv(filepath, ENCODINGS_TO_TRY)
        if df_raw is None:
            continue
        try:
            df_clean = clean_dataframe(df_raw)
            all_data_frames.append(df_clean)
        except Exception as e:
            # 這裡可以加入更詳細的錯誤訊息
            # print(f"處理檔案 {filepath.name} 失敗: {e}")
            continue

    if not all_data_frames:
        print(f"\n⚠️ 錯誤：{stock_name} 沒有可用的資料。")
        return None

    # 合併與排序
    combined_df = pd.concat(all_data_frames, ignore_index=True)
    # 由於原始資料中可能沒有 '日期' 欄位，這裡使用 drop_duplicates 時的 subset 應檢查
    combined_df = combined_df.drop_duplicates(subset=['日期']).copy()
    combined_df['日期'] = pd.to_datetime(combined_df['日期'], errors='coerce')
    combined_df = combined_df.dropna(subset=['日期']).copy()
    combined_df = combined_df.sort_values(by='日期', ascending=True).reset_index(drop=True)

    # MA 計算
    combined_df['MA5'] = combined_df['收盤價'].rolling(window=5, min_periods=1).mean().round(2)
    combined_df['MA10'] = combined_df['收盤價'].rolling(window=10, min_periods=1).mean().round(2)
    combined_df['MA20'] = combined_df['收盤價'].rolling(window=20, min_periods=1).mean().round(2)

    # 輸出 CSV
    # 準備字串格式的日期欄位
    combined_df['日期_str'] = combined_df['日期'].dt.strftime('%Y/%m/%d')
    
    # ⭐ 關鍵修改：不限制輸出欄位，將所有欄位（包含原始資料和新增的 MA 欄位）全部輸出
    output_path = output_dir / f"{stock_code}_{stock_name}_stocks_data.csv"
    try:
        # 注意：雖然您沒有傳入 PRICE_COLS，但在這裡已不再需要它。
        # 由於原始資料通常將日期放在最前面，且 MA 欄位在最後，故使用 all columns。
        combined_df.to_csv(output_path, index=False, encoding='big5') 
        print(f"✅ 已輸出 CSV 到：{output_path}")
    except Exception as e:
        print(f"❌ 輸出 CSV 失敗: {e}") 
        return None
        
    # 準備繪圖數據 (近 90 天) (此段保持不變，因為 mplfinance 的要求沒有變動)
    latest_date = combined_df['日期'].max()
    # 假設 timedelta 在您的環境中可用 (from datetime import timedelta)
    start_date = latest_date - timedelta(days=90)
    df_plot = combined_df.loc[combined_df['日期'] >= start_date].copy() 
    
    if df_plot.empty:
        print(f"⚠️ 近 90 天 ({start_date.date()} ~ {latest_date.date()}) 沒有足夠資料可以繪圖。")
        return None
        
    # 重新命名欄位並設定索引以符合 mplfinance 要求
    df_plot = df_plot.rename(columns={'成交股數': 'Volume', 
                                      '成交金額': 'Amount', 
                                      '開盤價': 'Open', 
                                      '最高價': 'High', 
                                      '最低價': 'Low', 
                                      '收盤價': 'Close', 
                                      '漲跌價差': 'Change', 
                                      '成交筆數': 'Trades'})
    df_plot = df_plot.set_index('日期')
    
    # 移除預先計算的 MA 欄位，讓 mplfinance 自己計算並繪圖
    for ma_col in ['MA5', 'MA10', 'MA20']:
        if ma_col in df_plot.columns:
            df_plot = df_plot.drop(columns=[ma_col])
            
    return df_plot

# -----------------------------
# 【主程式流程】
# -----------------------------
def main():
    print("--- 執行前準備：Matplotlib 字體與快取處理 ---")
    
    # 1. 強制清除 Matplotlib 字體快取
    try:
        cache_dir = Path(mpl.get_cachedir())
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            print(f"✅ Matplotlib 字體快取已清除 ({cache_dir.name})。下次執行將重建快取。")
    except Exception as e:
        print(f"⚠️ 無法自動清除 Matplotlib 快取: {e}")
    
    # 2. 設置字體屬性
    try:
        if FONT_PATH.exists():
            font_manager.fontManager.addfont(str(FONT_PATH))
            prop = font_manager.FontProperties(fname=str(FONT_PATH))
            plt.rcParams['font.family'] = prop.get_name()
            plt.rcParams['font.sans-serif'] = [prop.get_name()]
            print(f"✅ Matplotlib 已強制設置字體為：{prop.get_name()}")
        else:
            plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei', 'DejaVu Sans']
            print(f"✅ Matplotlib 字體設置為您指定的系統字體。")
            
        plt.rcParams['axes.unicode_minus'] = False 
    except Exception as e:
        print(f"❌ 字體設置失敗：{e}")
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        
    # 3. 確保輸出目錄存在
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"❌ 無法建立輸出資料夾 {OUTPUT_DIR}: {e}")
        return

    # 4. 載入並處理兩檔股票數據
    data_2344 = load_and_process_data(STOCK_LIST[0]['code'], STOCK_LIST[0]['name'], OUTPUT_DIR)
    data_1802 = load_and_process_data(STOCK_LIST[1]['code'], STOCK_LIST[1]['name'], OUTPUT_DIR)
    
    if data_2344 is None or data_1802 is None:
        print("\n❌ 由於資料缺失，無法繪製合併圖表。")
        return
        
    print("\n========================================================")
    print("📈 開始繪製左右並排 K 線圖 (定位於螢幕上半部)")
    print("========================================================")

    # 5. 設置繪圖樣式
    mc = mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', inherit=True)
    style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
    
    mav_periods = [5, 10, 20] 
    mavcolors = list(MA_COLORS.values()) 
    
    # 6. 創建總體 Figure (畫布) 和 1x2 的子圖 Axes (畫板)
    # 使用合適的尺寸 (寬20, 高10，適合上半部顯示)
    fig, axes = plt.subplots(1, 2, figsize=(20, 10)) 
    
    # 調整邊界以最大化圖表區域
    fig.subplots_adjust(left=0.03, right=0.97, top=0.90, bottom=0.1, wspace=0.1)
    
    # 建立自定義圖例物件
    legend_elements = [
        Line2D([0], [0], color=MA_COLORS['MA5'], lw=2, label='5日MA'),
        Line2D([0], [0], color=MA_COLORS['MA10'], lw=2, label='10日MA'),
        Line2D([0], [0], color=MA_COLORS['MA20'], lw=2, label='20日MA'),
    ]

    # 7. 繪製第一張圖 (2344 華邦電)
    mpf.plot(
        data_2344, 
        ax=axes[0], 
        type='candle', 
        mav=mav_periods, 
        mavcolors=mavcolors, 
        volume=False, 
        style=style,
        ylabel='價格 (TWD)',
    )
    axes[0].set_title(f"2344 ({STOCK_LIST[0]['name']}) 近 90 天 K 線 ({data_2344.index.min().date()} ~ {data_2344.index.max().date()})", fontsize=16)
    axes[0].legend(handles=legend_elements, loc='upper left', fontsize=12)
    
    # 8. 繪製第二張圖 (1802 台玻)
    mpf.plot(
        data_1802, 
        ax=axes[1], 
        type='candle', 
        mav=mav_periods, 
        mavcolors=mavcolors, 
        volume=False, 
        style=style,
        ylabel='價格 (TWD)'
    )
    axes[1].set_title(f"1802 ({STOCK_LIST[1]['name']}) 近 90 天 K 線 ({data_1802.index.min().date()} ~ {data_1802.index.max().date()})", fontsize=16)
    axes[1].legend(handles=legend_elements, loc='upper left', fontsize=12)

    
    # 9. 儲存合併後的圖表
    chart_filename = f"Combined_KLine_90Days_2344_1802.png"
    save_chart_path = OUTPUT_DIR / chart_filename
    try:
        fig.savefig(save_chart_path)
        print(f"📈 合併圖表已儲存到：{save_chart_path}")
    except Exception as e:
        print(f"❌ 合併圖表儲存失敗: {e}")

    # 10. 顯示圖表 (嘗試定位到螢幕上半部)
    print("\n👀 正在顯示左右並排的兩張 K 線圖 (定位於螢幕上半部)...")
    try:
        # 🌟 核心修正: 設置圖表大小和位置
        fig_manager = plt.get_current_fig_manager()

        # 設置視窗的絕對位置和大小 (寬度x高度+X偏移+Y偏移)
        # 假設我們需要一個 1800x900 的視窗 (與 figsize 20x10 比例接近)
        target_width = 1800
        target_height = 900
        
        # 假設螢幕寬度 W_screen，讓視窗居中: X_start = (W_screen - target_width) / 2
        # 這裡用 1920 當作參考，X_start 約為 (1920 - 1800) / 2 = 60
        start_x = 60 
        start_y = 50 # 距離頂部 50 像素

        geometry_string = f"{target_width}x{target_height}+{start_x}+{start_y}"
        
        try:
            # 適用於 TkAgg, QtAgg 等常見後端
            fig_manager.window.wm_geometry(geometry_string)
        except AttributeError:
            print("⚠️ Matplotlib 後端不支援手動設置視窗位置 (wm_geometry)。圖表將以預設位置顯示。")


        plt.show()
    except Exception as e:
        print(f"❌ 顯示圖表時發生錯誤: {e}")
    
    print("✅ 圖表顯示結束。所有任務完成。")

# 執行
if __name__ == '__main__':
    main()