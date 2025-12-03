# stock_data_process_flat_pathlib.py
# 目的：讀取多個原始 CSV（可能為民國年或西元年），清理、合併、計算 MA5/10/20，輸出 big5 編碼 CSV，並產生近90天 K 線圖。
# 風格：完全 Pathlib、函式單一功能、避免 lambda
# 顏色：上漲紅、下跌綠；MA5 紫、MA10 深綠、MA20 黃

import pandas as pd
from pathlib import Path
import re
from datetime import datetime, timedelta
import mplfinance as mpf
import matplotlib.pyplot as plt

# -----------------------------
# 設定
# -----------------------------
STOCK_CODE = "2330"
STOCK_NAME = "台積電"

BASE_RAW_DIR = Path(r"D:\Python_repo\python\Jason_Stock_Analyzer\datas\raw\1_STOCK_DAY")
INPUT_DIR = BASE_RAW_DIR / STOCK_CODE
OUTPUT_DIR = BASE_RAW_DIR.parent
OUTPUT_FILE = f"{STOCK_NAME}_stocks_data.csv"
OUTPUT_PATH = OUTPUT_DIR / OUTPUT_FILE

ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950']
PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
ALL_REQUIRED_COLS = ['日期'] + PRICE_COLS

# -----------------------------
# 函式：日期轉換
# -----------------------------
def convert_roc_to_gregorian(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None
    s = date_str.strip()
    trans_table = str.maketrans('，。／－：．', ',./-:.')
    s = s.translate(trans_table)
    s = s.replace('年','/').replace('月','/').replace('日','')
    m = re.search(r'(\d{2,4})\D+(\d{1,2})\D+(\d{1,2})', s)
    if not m:
        return None
    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year <= 1911:
        year += 1911
    try:
        d = datetime(year, month, day)
        return f"{d.year}/{d.month:02d}/{d.day:02d}"
    except Exception:
        return None

# -----------------------------
# 函式：多編碼讀 CSV
# -----------------------------
def try_read_csv(filepath: Path, encodings):
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            return df, enc
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"❌ {filepath.name} 使用 {enc} 發生錯誤: {e}")
            return None, None
    print(f"⚠️ {filepath.name} 無法讀取")
    return None, None

# -----------------------------
# 函式：清理 DataFrame
# -----------------------------
def clean_dataframe(df):
    df.columns = df.columns.str.strip()
    for col in ALL_REQUIRED_COLS:
        if col not in df.columns:
            raise KeyError(f"缺欄位: {col}")
    # 日期轉換
    date_list = []
    for d in df['日期'].astype(str):
        date_list.append(convert_roc_to_gregorian(d))
    df['日期'] = date_list
    df = df.dropna(subset=['日期']).copy()
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    df = df.dropna(subset=['日期']).copy()
    # 嚴格清理價格欄
    for col in PRICE_COLS:
        df[col] = df[col].astype(str).str.replace('，',',').str.replace('－','-')
        df[col] = df[col].astype(str).str.replace(r'[^0-9\.\-]','', regex=True)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=ALL_REQUIRED_COLS)
    return df[ALL_REQUIRED_COLS].copy()

# -----------------------------
# 函式：疊加 MA 線並加圖例
# -----------------------------
def add_ma_lines_and_legend(ax, df_plot, ma_colors, legend_loc='upper left', fontsize=10):
    for ma, color in ma_colors.items():
        if ma in df_plot.columns:
            ax.plot(df_plot.index, df_plot[ma], color=color, linewidth=1.5, label=ma)
    ax.legend(loc=legend_loc, fontsize=fontsize, frameon=True, shadow=True)

# -----------------------------
# 主程式
# -----------------------------
def main():
    if not INPUT_DIR.exists():
        print(f"❌ 資料夾不存在: {INPUT_DIR}")
        return
    files = sorted(INPUT_DIR.glob("*.csv"))
    if not files:
        print("⚠️ 沒有 CSV 檔")
        return

    all_dfs = []
    for f in files:
        print(f"\n--- 處理 {f.name} ---")
        df_raw, enc = try_read_csv(f, ENCODINGS_TO_TRY)
        if df_raw is None:
            continue
        print(f"使用編碼: {enc}")
        try:
            df_clean = clean_dataframe(df_raw)
        except Exception as e:
            print(f"❌ 清理失敗: {e}")
            continue
        all_dfs.append(df_clean)

    if not all_dfs:
        print("⚠️ 沒有可用資料")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['日期'])
    combined_df['日期'] = pd.to_datetime(combined_df['日期'], errors='coerce')
    combined_df = combined_df.dropna(subset=['日期']).sort_values(by='日期').reset_index(drop=True)
    print(f"資料範圍: {combined_df['日期'].min()} ~ {combined_df['日期'].max()} 共 {len(combined_df)} 筆")

    # 計算 MA
    combined_df['MA5'] = combined_df['收盤價'].rolling(5, min_periods=1).mean().round(2)
    combined_df['MA10'] = combined_df['收盤價'].rolling(10, min_periods=1).mean().round(2)
    combined_df['MA20'] = combined_df['收盤價'].rolling(20, min_periods=1).mean().round(2)

    # 輸出 CSV
    combined_df['日期_str'] = combined_df['日期'].dt.strftime('%Y/%m/%d')
    combined_df.to_csv(OUTPUT_PATH, index=False, encoding='big5', columns=['日期_str']+PRICE_COLS+['MA5','MA10','MA20'])
    print(f"✅ 已輸出 CSV: {OUTPUT_PATH}")

    # 繪圖
    latest_date = combined_df['日期'].max()
    df_plot_full = combined_df[combined_df['日期'] >= latest_date - timedelta(days=90)].copy()
    if df_plot_full.empty:
        print("⚠️ 資料不足以繪圖")
        return

    # 顯示前 60 天
    df_plot = df_plot_full.tail(60)
    df_plot = df_plot.rename(columns={'開盤價':'Open','最高價':'High','最低價':'Low','收盤價':'Close'}).set_index('日期')

    # K 線樣式
    mc = mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', inherit=True)
    style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
    mav = [5,10,20]
    mavcolors = ['purple','darkgreen','gold']
    title = f"{STOCK_CODE} ({STOCK_NAME}) 近 90 天 K 線 ({df_plot.index.min().date()}~{df_plot.index.max().date()})"

    try:
        fig, axlist = mpf.plot(df_plot, type='candle', mav=mav, mavcolors=mavcolors, volume=False,
                               style=style, title=title, ylabel='價格 (TWD)', figscale=1.5, returnfig=True)
    except TypeError:
        fig, axlist = mpf.plot(df_plot, type='candle', mav=mav, volume=False, style=style,
                               title=title, ylabel='價格 (TWD)', figscale=1.5, returnfig=True)
        main_ax = axlist[0] if isinstance(axlist, list) else axlist
        add_ma_lines_and_legend(main_ax, df_plot, {'MA5':'purple','MA10':'darkgreen','MA20':'gold'})

    # 字體設定
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei','Microsoft YaHei','DejaVu Sans']

    # 圖檔存檔
    chart_path = OUTPUT_DIR / f"{STOCK_CODE}_KLine_MA_Combined.png"
    fig.savefig(chart_path)
    print(f"📈 圖表已儲存: {chart_path}")
    plt.show()
    print("\n🎉 完成！")

if __name__ == '__main__':
    main()
