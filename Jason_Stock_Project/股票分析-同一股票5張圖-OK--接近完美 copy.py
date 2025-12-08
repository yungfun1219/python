import pandas as pd
from pathlib import Path
import re
from datetime import datetime, timedelta
import os
import sys
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# =============================================================
# 🎯 設定區塊：請根據您的檔案結構和股票資訊修改
# =============================================================
STOCK_CODE = "8110"
STOCK_NAME = "華東"
#STOCK_CODE = "2344"
#STOCK_NAME = "華邦電"



# ------------------------------------
# 📌 1. 路徑設定 (保持不變)
# ------------------------------------
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))) 
    
BASE_RAW_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"
INPUT_DIR = BASE_RAW_DIR / STOCK_CODE

# 輸出路徑
# 請確保此路徑存在或程式有權限建立
OUTPUT_DIR = Path("D:/Python_repo/python/Jason_Stock_Project/datas/processed/stock_all") 
OUTPUT_FILE = f"{STOCK_CODE}_{STOCK_NAME}_stocks_data.csv"
OUTPUT_PATH = OUTPUT_DIR / OUTPUT_FILE

# ------------------------------------
# 📌 2. 欄位與指標設定 (保持不變)
# ------------------------------------
ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950']

# 🎯 需要保留的所有原始欄位
RAW_COL_NAMES = ['日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']

# 價格欄位 (用於數值轉換和 MACD/MA 計算)
PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']

# MACD 參數設定
SHORT_WINDOW = 12
LONG_WINDOW = 26
SIGNAL_WINDOW = 9

# KDJ 參數設定
KDJ_N = 9 # KDJ 週期 (通常是 9 天)
KDJ_M1 = 3 # K 的平滑週期 (通常是 3 天)
KDJ_M2 = 3 # D 的平滑週期 (通常是 3 天)

# RSI 參數設定
RSI_PERIODS = [5, 10] # 要求的 RSI 週期

# VOL 參數設定
VOL_PERIODS = [5, 10]

# MA 線的顏色定義 (用於 mplfinance 和 MACD 圖例)
MA_COLORS = {
    'MA5': 'purple', 
    'MA10': 'darkgreen', 
    'MA20': 'gold'
}
MAV_PERIODS = [5, 10, 20]
MAV_COLORS_LIST = list(MA_COLORS.values())
# =============================================================

# -----------------------------------------------------------
# 【資料處理函式 - 保持不變】
# -----------------------------------------------------------

def convert_roc_to_gregorian(date_str):
    """將多種日期格式轉為 YYYY/MM/DD 格式字串。"""
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
        
    if year > 1911:
        greg_year = year
    else:
        greg_year = year + 1911
        
    try:
        d = datetime(greg_year, month, day)
        return f"{d.year}/{d.month:02d}/{d.day:02d}"
    except ValueError:
        return None

def try_read_csv(filepath, encodings):
    """嘗試使用多種編碼讀取 CSV 檔案。"""
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

def clean_dataframe(df, required_raw_cols, price_cols):
    """對單一 DataFrame 進行欄位清理、日期轉換和價格數值化。保留所有指定的原始欄位，並確保關鍵欄位存在。"""
    df.columns = df.columns.str.strip()
    
    col_mapping = {}
    missing_cols = []
    
    for col in required_raw_cols:
        found = False
        for df_col in df.columns:
            # 寬鬆匹配: 忽略空格、特殊符號，只匹配關鍵中文名稱
            cleaned_df_col = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5]', '', df_col)
            cleaned_std_col = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5]', '', col)
            
            if cleaned_df_col == cleaned_std_col:
                col_mapping[df_col] = col
                found = True
                break
        
        if not found:
            missing_cols.append(col)
            
    if missing_cols:
        raise KeyError(f"資料缺少關鍵欄位: {missing_cols}")
        
    df = df.rename(columns=col_mapping)
    df = df[required_raw_cols].copy()
    
    # 日期轉換
    df['日期'] = df['日期'].astype(str).apply(convert_roc_to_gregorian)
    df = df.dropna(subset=['日期']).copy()
    df['日期'] = pd.to_datetime(df['日期'], format='%Y/%m/%d', errors='coerce')
    df = df.dropna(subset=['日期']).copy()
    
    # 數值欄位清理
    for col in required_raw_cols:
        if col == '日期':
            continue
            
        df[col] = df[col].astype(str).str.replace('，', ',', regex=False).str.replace('－', '-', regex=False)
        df[col] = df[col].astype(str).str.replace(r'[^0-9\.\-]', '', regex=True)
        df[col] = df[col].replace({'': pd.NA, 'nan': pd.NA, 'NaN': pd.NA})
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    # 確保價格欄位不是 NaN (這是計算技術指標的基礎)
    df = df.dropna(subset=price_cols).copy() 
    
    return df[required_raw_cols].copy()

def calculate_moving_averages(df, close_col='收盤價'):
    """計算 MA5, MA10, MA20。"""
    df['MA5'] = df[close_col].rolling(window=5, min_periods=1).mean().round(2)
    df['MA10'] = df[close_col].rolling(window=10, min_periods=1).mean().round(2)
    df['MA20'] = df[close_col].rolling(window=20, min_periods=1).mean().round(2)
    return df

def calculate_macd(df, close_col='收盤價', short=12, long=26, signal=9):
    """計算 MACD 三項指標 (DIF, DEA, OSC)。"""
    df['EMA_12'] = df[close_col].ewm(span=short, adjust=False).mean()
    df['EMA_26'] = df[close_col].ewm(span=long, adjust=False).mean()
    df['DIF'] = df['EMA_12'] - df['EMA_26']
    df['DEA'] = df['DIF'].ewm(span=signal, adjust=False).mean()
    df['OSC'] = df['DIF'] - df['DEA']
    
    df.drop(columns=['EMA_12', 'EMA_26'], inplace=True, errors='ignore')
    return df
    
def calculate_kdj(df, n=9, m1=3, m2=3, high_col='最高價', low_col='最低價', close_col='收盤價'):
    """計算 KDJ 三項指標 (K, D, J)。"""
    # 1. 計算 N 日內的最高價 (HHV) 和最低價 (LLV)
    df['LLV'] = df[low_col].rolling(window=n, min_periods=1).min()
    df['HHV'] = df[high_col].rolling(window=n, min_periods=1).max()

    # 2. 計算 RSV (未成熟隨機值)
    denominator = df['HHV'] - df['LLV']
    df['RSV'] = ((df[close_col] - df['LLV']) / denominator)
    df['RSV'] = df['RSV'].replace([float('inf'), -float('inf')], 1).fillna(0) * 100
    df['RSV'] = df['RSV'].clip(0, 100) 

    # 3. 計算 K 線和 D 線 (使用經典平滑移動平均法)
    k_list = []
    d_list = []
    
    k_prev = 50.0 # 初始值
    d_prev = 50.0 # 初始值
    
    rsv_values = df['RSV'].values
    m1_float = float(m1)
    m2_float = float(m2)
    
    for rsv in rsv_values:
        # K = (M1-1)/M1 * K_prev + 1/M1 * RSV_current
        k_curr = (k_prev * (m1_float - 1) + rsv) / m1_float
        
        # D = (M2-1)/M2 * D_prev + 1/M2 * K_current
        d_curr = (d_prev * (m2_float - 1) + k_curr) / m2_float
        
        k_list.append(round(k_curr, 2))
        d_list.append(round(d_curr, 2))
        
        k_prev = k_curr
        d_prev = d_curr

    df['K'] = k_list
    df['D'] = d_list
    
    # 4. 計算 J 線
    df['J'] = (3 * df['K'] - 2 * df['D']).round(2)

    # 刪除計算中間過程欄位
    df.drop(columns=['LLV', 'HHV', 'RSV'], inplace=True, errors='ignore')
    return df

def calculate_rsi(df, periods, close_col='收盤價'):
    """計算 RSI 指標及 RSI_DIF (RSI5 - RSI10)。"""
    # 計算價格變化
    df['Change'] = df[close_col].diff()
    
    # 分離上漲 (Gain) 和下跌 (Loss)
    df['Gain'] = df['Change'].apply(lambda x: x if x > 0 else 0).round(2)
    df['Loss'] = df['Change'].apply(lambda x: abs(x) if x < 0 else 0).round(2)
    
    for period in periods:
        # 使用 ewm (指數移動平均) 進行平滑
        avg_gain = df['Gain'].ewm(span=period, adjust=False).mean()
        avg_loss = df['Loss'].ewm(span=period, adjust=False).mean()
        
        # 計算 RS (相對強度)，避免除以零
        rs = avg_gain / avg_loss.replace(0, 1e-10) 
        
        # 計算 RSI
        rsi = 100 - (100 / (1 + rs))
        df[f'RSI{period}'] = rsi.round(2)

    # 刪除中間計算過程欄位
    df.drop(columns=['Change', 'Gain', 'Loss'], inplace=True, errors='ignore')
    
    # 計算 RSI_DIF = RSI5 - RSI10
    if 'RSI5' in df.columns and 'RSI10' in df.columns:
        df['RSI_DIF'] = (df['RSI5'] - df['RSI10']).round(2)
    else:
        print("⚠️ 警告：缺少 RSI5 或 RSI10 欄位，RSI_DIF 計算失敗。")

    return df

def calculate_volume_moving_averages(df, volume_col='成交股數', periods=VOL_PERIODS):
    """計算成交股數的移動平均 (VOL5, VOL10)。"""
    for period in periods:
        # 成交量移動平均通常取整數
        df[f'VOL{period}'] = df[volume_col].rolling(window=period, min_periods=1).mean().round(0)
    return df

def filter_recent_data(df, days=90):
    """從 DataFrame 中篩選出最近 N 天的數據。"""
    if df.empty:
        return df
    latest_date = df['日期'].max()
    start_date = latest_date - timedelta(days=days) 
    final_df = df[df['日期'] >= start_date].copy() 
    return final_df

def output_to_csv(df, output_path, raw_cols):
    """將 DataFrame 輸出為 CSV 檔案 (big5 編碼)。"""
    # 將日期格式化為字串
    df['日期'] = df['日期'].dt.strftime('%Y/%m/%d')
    df = df.rename(columns={'日期': '日期_str'})
    
    # 確定輸出的欄位順序：日期_str + 所有原始欄位 (排除日期) + 技術指標
    tech_cols = ['MA5', 'MA10', 'MA20', 'DIF', 'DEA', 'OSC', 'K', 'D', 'J', 'RSI5', 'RSI10', 'RSI_DIF', 'VOL5', 'VOL10']
    
    output_cols = ['日期_str'] + [col for col in raw_cols if col != '日期'] + tech_cols
    
    df_output = df.reindex(columns=output_cols).copy()
    
    try:
        df_output.to_csv(output_path, index=False, encoding='big5')
        print(f"✅ 已輸出 CSV（big5，包含所有原始欄位/MA/MACD/KDJ/RSI/VOL）到：{output_path}")
        return True
    except Exception as e:
        print(f"❌ 輸出 CSV 失敗: {e}")
        return False

# -----------------------------------------------------------
# 【🌟 最終修正後的繪圖函式：加入全螢幕/最大化顯示邏輯 & 移除 VOL 科學記號】
# -----------------------------------------------------------
def plot_charts_four_panels(df, code, name, output_dir):
    """
    繪製 K 線圖、MACD、KDJ、RSI、VOL 的 5x1 圖表。
    - VOL 圖：VOL5/VOL10 曲線 + 成交股數柱狀圖 (顏色依漲跌價差決定)。
    - 移除 VOL 圖 Y 軸的科學記號 (1e8)。
    - 圖表在 plt.show() 時，視窗嘗試最大化。
    """
    if df.empty:
        print("❌ 繪圖失敗：數據不足。")
        return

    # 1. 準備 mplfinance K 線圖資料
    df_ohlc = df.rename(columns={'開盤價': 'Open', '最高價': 'High', '最低價': 'Low', '收盤價': 'Close'}).set_index('日期').copy()
    required_cols = ['DIF', 'DEA', 'OSC', 'K', 'D', 'J', 'RSI5', 'RSI10', 'VOL5', 'VOL10', '成交股數', '漲跌價差']
    df_tech = df_ohlc.dropna(subset=[col for col in required_cols if col in df_ohlc.columns]).copy()
    
    if df_tech.empty:
        print("❌ 繪圖失敗：數據不足以繪製技術指標圖表。")
        return
        
    # --- 字體設定 ---
    try:
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei', 'SimHei', 'Apple LiGothic', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False 
    except Exception:
        pass

    # --- 繪圖樣式 ---
    mc = mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', inherit=True)
    style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
    
    # --- 創建 Figure 和 Axes (5x1 佈局) ---
    fig, axes = plt.subplots(5, 1, figsize=(16, 22), 
                             gridspec_kw={'height_ratios': [3, 1, 1, 1, 1]}) 
    
    fig.subplots_adjust(left=0.06, right=0.97, top=0.94, bottom=0.04, hspace=0.3)
    
    ax_kline = axes[0] 
    ax_macd = axes[1]  
    ax_kdj = axes[2]   
    ax_rsi = axes[3]   
    ax_vol = axes[4]   
    
    start_date = df_ohlc.index.min().date()
    end_date = df_ohlc.index.max().date()
    fig.suptitle(f'{code} ({name}) 技術分析總覽 ({start_date} ~ {end_date})', fontsize=20, y=0.98)
    
    # --- 1. K 線圖 (第一行) ---
    mpf.plot(
        df_ohlc, 
        ax=ax_kline, 
        type='candle', 
        mav=MAV_PERIODS, 
        mavcolors=MAV_COLORS_LIST, 
        volume=False, 
        style=style,
        ylabel='股價 (TWD)',
        xrotation=0,
        show_nontrading=False,
        datetime_format='%Y/%m/%d',
    )
    
    legend_handles = []
    ma_labels = [f'MA{p}' for p in MAV_PERIODS]
    for color, label in zip(MAV_COLORS_LIST, ma_labels):
        legend_handles.append(plt.Line2D([0], [0], color=color, linewidth=1.5, label=label))
    
    ax_kline.legend(handles=legend_handles, loc='upper left', fontsize=10) 
    ax_kline.set_title('1. 股價 K 線圖與移動平均線 (MA)', fontsize=14, loc='left')
    ax_kline.set_xlabel('')
    
    # --- 2. MACD 指標圖 (第二行) ---
    ax_macd.plot(df_tech.index, df_tech['DIF'], color='blue', linewidth=1.5, label='DIF (差離值)')
    ax_macd.plot(df_tech.index, df_tech['DEA'], color='orange', linewidth=1.5, label='DEA (訊號線)')
    osc_colors = ['r' if val >= 0 else 'g' for val in df_tech['OSC']]
    ax_macd.bar(df_tech.index, df_tech['OSC'], color=osc_colors, alpha=0.6, label='OSC (柱)') 
    
    ax_macd.set_title('2. MACD 指標 (DIF, DEA, OSC)', fontsize=14, loc='left')
    ax_macd.axhline(0, color='gray', linestyle='--', linewidth=1) 
    ax_macd.legend(loc='upper left', fontsize=10) 
    ax_macd.grid(True, linestyle='--', alpha=0.6)
    ax_macd.set_xlabel('')

    # --- 3. KDJ 指標線圖 (第三行) ---
    ax_kdj.plot(df_tech.index, df_tech['K'], color='red', linewidth=1.5, label='K 線')
    ax_kdj.plot(df_tech.index, df_tech['D'], color='green', linewidth=1.5, label='D 線')
    ax_kdj.plot(df_tech.index, df_tech['J'], color='blue', linewidth=1.5, label='J 線')
    
    ax_kdj.set_title('3. 隨機指標 KDJ 線圖', fontsize=14, loc='left')
    ax_kdj.axhline(80, color='red', linestyle=':', linewidth=1) 
    ax_kdj.axhline(20, color='green', linestyle=':', linewidth=1) 
    ax_kdj.set_ylim(0, 100) 
    ax_kdj.legend(loc='upper left', fontsize=10) 
    ax_kdj.grid(True, linestyle='--', alpha=0.6)
    ax_kdj.set_xlabel('') 

    # --- 4. RSI 指標線圖 (第四行) ---
    ax_rsi.plot(df_tech.index, df_tech['RSI5'], color='darkorange', linewidth=1.5, label='RSI5')
    ax_rsi.plot(df_tech.index, df_tech['RSI10'], color='purple', linewidth=1.5, label='RSI10')
    
    ax_rsi.set_title('4. 相對強弱指標 RSI 線圖', fontsize=14, loc='left')
    ax_rsi.axhline(70, color='red', linestyle=':', linewidth=1) 
    ax_rsi.axhline(30, color='green', linestyle=':', linewidth=1) 
    ax_rsi.set_ylim(0, 100) 
    ax_rsi.legend(loc='upper left', fontsize=10) 
    ax_rsi.grid(True, linestyle='--', alpha=0.6)
    ax_rsi.set_xlabel('') 
    
    # --- 5. VOL 成交量圖 (第五行) ---
    vol_colors = ['r' if val > 0 else 'g' for val in df_tech['漲跌價差']]
    
    ax_vol.bar(df_tech.index, df_tech['成交股數'], color=vol_colors, alpha=0.6, label='成交股數 (VOL)')
    
    ax_vol_curve = ax_vol.twinx()
    ax_vol_curve.plot(df_tech.index, df_tech['VOL5'], color='yellow', linewidth=1.5, label='VOL5')
    ax_vol_curve.plot(df_tech.index, df_tech['VOL10'], color='orange', linewidth=1.5, label='VOL10')
    
    ax_vol.set_title('5. 成交量指標 (VOL5/VOL10 曲線與成交股數柱狀圖)', fontsize=14, loc='left')
    
    # 🌟 核心修正 1：移除主 Y 軸 (左側，成交股數) 的科學記號
    ax_vol.ticklabel_format(style='plain', axis='y') 
    
    # 🌟 核心修正 2：移除次 Y 軸 (右側，VOL5/10) 的科學記號
    ax_vol_curve.ticklabel_format(style='plain', axis='y')
    
    handles1, labels1 = ax_vol.get_legend_handles_labels()
    handles2, labels2 = ax_vol_curve.get_legend_handles_labels()
    ax_vol.legend(handles1 + handles2, labels1 + labels2, loc='upper left', fontsize=10) 
    
    ax_vol.grid(True, linestyle='--', alpha=0.6)
    ax_vol.set_xlabel('日期', fontsize=12) 

    # --- 統一處理 X 軸：隱藏前四張圖的 X 軸刻度和標籤 ---
    for i in range(len(axes) - 1): 
        axes[i].tick_params(axis='x', labelbottom=False) 
        
    ax_vol.tick_params(axis='x', rotation=0) 
    ax_vol.xaxis.set_major_locator(plt.MaxNLocator(10))

    # --- 儲存與顯示 ---
    chart_filename = f"{code}_{name}_KLine_5Panel_90Days.png"
    save_path = output_dir / chart_filename
    try:
        fig.savefig(save_path)
        print(f"📈 整合圖表 (5 圖面板) 已儲存到：{save_path}")
    except Exception as e:
        print(f"❌ 整合圖表儲存失敗: {e}")
        
    # 核心修改區：嘗試最大化顯示視窗
    try:
        fig_manager = plt.get_current_fig_manager()
        
        # 1. 嘗試針對不同後端進行最大化 (支援 TkAgg, QtAgg 等)
        try:
            # 適用於 TkAgg 後端 (Windows/Linux 上常見)
            fig_manager.window.state('zoomed') 
            print("視窗已嘗試最大化 (TkAgg 模式)。")
        except AttributeError:
            try:
                # 適用於 QtAgg/MacOSX 等後端
                fig_manager.frame.Maximize(True)
                print("視窗已嘗試最大化 (Qt/Wx 模式)。")
            except AttributeError:
                # 回退到設定一個較大的尺寸，以達到接近全螢幕的效果
                target_width = 1600
                target_height = 1200
                start_x = 0 
                start_y = 0
                geometry_string = f"{target_width}x{target_height}+{start_x}+{start_y}"
                fig_manager.window.wm_geometry(geometry_string)
                print("視窗最大化功能失敗，已回退設置為較大尺寸。")
        except Exception as e:
            print(f"⚠️ 視窗最大化功能失敗，原因: {e}")
            
        plt.show()
    except Exception as e:
        print(f"❌ 顯示圖表時發生錯誤: {e}")

# -----------------------------
# 主流程：流程扁平化 (保持不變)
# -----------------------------
def main():
    # 0. 檢查資料夾並確保輸出目錄存在
    if not INPUT_DIR.exists():
        print(f"❌ 指定的原始資料夾不存在：{INPUT_DIR}")
        return
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True) 
    
    file_list = sorted([p for p in INPUT_DIR.iterdir() if p.suffix.lower() == '.csv'])
    if not file_list:
        print("⚠️ 資料夾內沒有 csv 檔案。")
        return

    # 1. 讀取、清理單個檔案並收集
    all_data_frames = []
    for filepath in file_list:
        print(f"\n--- 處理檔案: {filepath.name} ---")
        df_raw, used_encoding = try_read_csv(filepath, ENCODINGS_TO_TRY)
        if df_raw is None:
            continue
        print(f"    使用編碼: {used_encoding}")
        try:
            df_clean = clean_dataframe(df_raw, RAW_COL_NAMES, PRICE_COLS) 
            all_data_frames.append(df_clean)
        except Exception as e:
            print(f"❌ 檔案 {filepath.name} 清理失敗: {e}")
            continue

    if not all_data_frames:
        print("\n⚠️ 錯誤：沒有可用的資料。")
        return

    # 2. 合併、排序與去重 (全量數據)
    combined_df = pd.concat(all_data_frames, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['日期']).copy()
    combined_df = combined_df.sort_values(by='日期', ascending=True).reset_index(drop=True)
    full_combined_df = combined_df.copy()
    print("\n--- 資料合併與排序完成 ---")
    print(f"全量資料區間: {full_combined_df['日期'].min().date()} ~ {full_combined_df['日期'].max().date()}，共 {len(full_combined_df)} 筆")

    # 3. 計算技術指標 (MA/MACD/KDJ/RSI/VOL)
    full_combined_df = calculate_moving_averages(full_combined_df, close_col='收盤價')
    full_combined_df = calculate_macd(
        full_combined_df, 
        close_col='收盤價', 
        short=SHORT_WINDOW, 
        long=LONG_WINDOW, 
        signal=SIGNAL_WINDOW
    )
    full_combined_df = calculate_kdj(
        full_combined_df,
        n=KDJ_N,
        m1=KDJ_M1,
        m2=KDJ_M2
    )
    full_combined_df = calculate_rsi(
        full_combined_df,
        periods=RSI_PERIODS
    )
    full_combined_df = calculate_volume_moving_averages(
        full_combined_df,
        volume_col='成交股數',
        periods=VOL_PERIODS
    )
    print("--- MA/MACD/KDJ/RSI/VOL 計算完成 ---")

    # 4. 篩選：只保留最近 90 天（三個月）的資料用於繪圖 (CSV 輸出使用全量)
    final_df = filter_recent_data(full_combined_df, days=90)
    
    if final_df.empty:
        print(f"\n❌ 錯誤：篩選後沒有資料。")
        output_to_csv(full_combined_df.copy(), OUTPUT_PATH, RAW_COL_NAMES) 
        return
    print(f"\n--- 最終分析範圍篩選完成 ---")
    print(f"篩選後日期區間: {final_df['日期'].min().date()} ~ {final_df['日期'].max().date()}，共 {len(final_df)} 筆")


    # 5. 輸出 CSV (使用所有原始欄位和全量數據)
    output_to_csv(full_combined_df.copy(), OUTPUT_PATH, RAW_COL_NAMES) 

    # 6. 繪圖：整合 K 線、MACD、KDJ、RSI、VOL 指標圖表 (使用 90 天資料)
    plot_charts_four_panels(final_df.copy(), STOCK_CODE, STOCK_NAME, OUTPUT_DIR)
        
    print("\n🎉 任務完成。")

# 執行
if __name__ == '__main__':
    main()