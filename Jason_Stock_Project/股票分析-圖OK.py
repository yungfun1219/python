import pandas as pd
from pathlib import Path
import re
from datetime import datetime, timedelta
import os
import sys
import mplfinance as mpf
import matplotlib.pyplot as plt

# 設定（可修改）
STOCK_CODE = "2344"
STOCK_NAME = "華邦電"
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
BASE_RAW_DIR = BASE_DIR / "datas" / "raw" / "1_STOCK_DAY"
INPUT_DIR = BASE_RAW_DIR / STOCK_CODE
OUTPUT_DIR = BASE_RAW_DIR.parent  # 與原程式行為一致
OUTPUT_FILE = f"{STOCK_NAME}_stocks_data.csv"
OUTPUT_PATH = OUTPUT_DIR / OUTPUT_FILE

ENCODINGS_TO_TRY = ['utf-8-sig', 'big5', 'utf-8', 'cp950']
PRICE_COLS = ['開盤價', '最高價', '最低價', '收盤價']
ALL_REQUIRED_COLS = ['日期'] + PRICE_COLS

# -----------------------------
# 函式：將民國或多種格式的日期字串轉成 YYYY/MM/DD（字串）
# 單一功能：不呼叫其他自定義函式
# -----------------------------
def convert_roc_to_gregorian(date_str):
    """
    支援範例輸入：
      '112/10/31', '112/1/2', '2024/11/05', '2024-11-05', '民國112年10月31日', '2024.11.05'
    回傳： 'YYYY/MM/DD' 或 None
    """
    if pd.isna(date_str):
        return None
    if not isinstance(date_str, str):
        return None
    s = date_str.strip()
    # 將常見全形符號轉半形
    trans_table = str.maketrans('，。／－：．', ',./-:.')
    s = s.translate(trans_table)
    # 將中文年/月/日轉 '/'
    s = s.replace('年', '/').replace('月', '/').replace('日', '')
    # 嘗試用 regex 抽出三個數字群（年、月、日）
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
    except Exception:
        return None
    if year > 1911:
        greg_year = year
    else:
        greg_year = year + 1911
    try:
        d = datetime(greg_year, month, day)
        return f"{d.year}/{d.month:02d}/{d.day:02d}"
    except Exception:
        return None

# -----------------------------
# 單一功能：嘗試以多種編碼讀 CSV 檔，回傳 DataFrame 與使用的 encoding，失敗回傳 (None, None)
# -----------------------------
def try_read_csv(filepath, encodings):
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, encoding=enc, header=0)
            return df, enc
        except UnicodeDecodeError:
            continue
        except Exception as e:
            # 若為其他錯誤（例如格式錯誤），印出並跳出嘗試
            print(f"❌ {filepath.name} 使用 {enc} 讀取時發生非編碼錯誤: {e}")
            return None, None
    print(f"⚠️ {filepath.name} 無法用指定編碼讀取。")
    return None, None

# -----------------------------
# 單一功能：對單一 DataFrame 清理欄位、轉日期、清理價格，並回傳只含必要欄位之 DataFrame
# -----------------------------
def clean_dataframe(df):
    # 移除欄位首尾空白
    df.columns = df.columns.str.strip()
    # 檢查必要欄位
    for col in ALL_REQUIRED_COLS:
        if col not in df.columns:
            raise KeyError(f"資料缺少欄位: {col}")
    # 轉換日期欄（用迴圈而非 lambda）
    date_list = []
    for orig in df['日期'].astype(str):
        conv = convert_roc_to_gregorian(orig)
        date_list.append(conv)
    df['日期'] = date_list
    # drop 無法解析日期的列
    df = df.dropna(subset=['日期']).copy()
    # 轉成 datetime
    df['日期'] = pd.to_datetime(df['日期'], format='%Y/%m/%d', errors='coerce')
    df = df.dropna(subset=['日期']).copy()
    # 嚴格清理價格欄（移除非數字與小數點、負號）
    for col in PRICE_COLS:
        # 可能有全形逗號或中文破折號，先替換
        df[col] = df[col].astype(str).str.replace('，', ',', regex=False).str.replace('－', '-', regex=False)
        # 用 regex 移除非數字、小數點、負號
        df[col] = df[col].astype(str).str.replace(r'[^0-9\.\-]', '', regex=True)
        # 空字串視為 NA
        df[col] = df[col].replace({'': pd.NA, 'nan': pd.NA, 'NaN': pd.NA})
        # 轉數值（無法轉的會成 NaN）
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # drop 若關鍵價格變成 NA 的列
    df = df.dropna(subset=ALL_REQUIRED_COLS).copy()
    # 保留必要欄位
    return df[ALL_REQUIRED_COLS].copy()

# -----------------------------
# 主流程（扁平化）：讀所有檔案 -> 清理 -> 合併 -> 計算 MA -> 寫檔（big5） -> 繪圖
# -----------------------------
def main():
    if not INPUT_DIR.exists():
        print(f"❌ 指定的資料夾不存在：{INPUT_DIR}")
        return
    all_data_frames = []
    file_list = sorted([p for p in INPUT_DIR.iterdir() if p.suffix.lower() == '.csv'])
    if not file_list:
        print("⚠️ 資料夾內沒有 csv 檔案。")
        return

    for filepath in file_list:
        print(f"\n--- 處理檔案: {filepath.name} ---")
        df_raw, used_encoding = try_read_csv(filepath, ENCODINGS_TO_TRY)
        if df_raw is None:
            continue
        print(f"    使用編碼: {used_encoding}")
        try:
            df_clean = clean_dataframe(df_raw)
        except Exception as e:
            print(f"❌ 檔案 {filepath.name} 清理失敗: {e}")
            continue
        # 列出解析到的日期範圍
        parsed_dates = pd.to_datetime(df_clean['日期'], errors='coerce')
        min_d = parsed_dates.min()
        max_d = parsed_dates.max()
        print(f"    解析日期範圍: {min_d} ~ {max_d} (共 {len(parsed_dates)} 筆)")
        all_data_frames.append(df_clean)

    if not all_data_frames:
        print("\n⚠️ 錯誤：沒有可用的資料。")
        return

    combined_df = pd.concat(all_data_frames, ignore_index=True)
    # 移除同日重複（保留第一個出現）
    combined_df = combined_df.drop_duplicates(subset=['日期']).copy()
    # 轉 datetime（若尚未）
    combined_df['日期'] = pd.to_datetime(combined_df['日期'], errors='coerce')
    combined_df = combined_df.dropna(subset=['日期']).copy()
    combined_df = combined_df.sort_values(by='日期', ascending=True).reset_index(drop=True)
    print("\n--- 資料合併與排序完成 ---")
    print(f"資料日期區間: {combined_df['日期'].min()} ~ {combined_df['日期'].max()}，共 {len(combined_df)} 筆")

    # 偵測不合理年份（例如 >3000）
    weird = combined_df[combined_df['日期'].dt.year > 3000]
    if not weird.empty:
        print("⚠️ 發現不合理日期（年份>3000），範例列出：")
        print(weird.head(10).to_string())

    # 計算移動平均
    combined_df['MA5'] = combined_df['收盤價'].rolling(window=5, min_periods=1).mean().round(2)
    combined_df['MA10'] = combined_df['收盤價'].rolling(window=10, min_periods=1).mean().round(2)
    combined_df['MA20'] = combined_df['收盤價'].rolling(window=20, min_periods=1).mean().round(2)
    print("--- MA5/MA10/MA20 計算完成 ---")

    # 輸出 CSV（big5 編碼）
    # 加一個輸出欄位格式化日期字串
    combined_df['日期_str'] = combined_df['日期'].dt.strftime('%Y/%m/%d')
    output_cols = ['日期_str'] + PRICE_COLS + ['MA5', 'MA10', 'MA20']
    try:
        combined_df.to_csv(OUTPUT_PATH, index=False, encoding='big5', columns=output_cols)
        print(f"✅ 已輸出 CSV（big5）到：{OUTPUT_PATH}")
    except Exception as e:
        print(f"❌ 輸出 CSV 失敗: {e}")

    # 繪圖：近 90 天 K 線（若有足夠資料）
    latest_date = combined_df['日期'].max()
    start_date = latest_date - timedelta(days=90)
    df_plot = combined_df[combined_df['日期'] >= start_date].copy()
    if df_plot.empty:
        print(f"⚠️ 近 90 天 ({start_date.date()} ~ {latest_date.date()}) 沒有足夠資料可以繪圖。")
        return

    # 準備 OHLC DataFrame
    df_plot = df_plot.rename(columns={'開盤價': 'Open', '最高價': 'High', '最低價': 'Low', '收盤價': 'Close'})
    df_plot = df_plot.set_index('日期')

    # K 線樣式（紅漲綠跌）
    mc = mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', inherit=True)
    style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
    mav = [5, 10, 20]
    # 新增：指定 MA 線顏色（順序對應 mav）
    mavcolors = ['purple', 'darkgreen', 'gold']
    title = f"{STOCK_CODE} ({STOCK_NAME}) 近 90 天 K 線 ({df_plot.index.min().date()} ~ {latest_date.date()})"

    # 傳入 mavcolors 以指定 MA 顏色
    try:
        fig, axes = mpf.plot(df_plot, type='candle', mav=mav, mavcolors=mavcolors, volume=False, style=style,
                             title=title, ylabel='價格 (TWD)', figscale=1.5, returnfig=True)
    except TypeError:
        # 若 mplfinance 版本不支援 mavcolors，改為不帶 mavcolors 並在主軸疊加 MA 線
        fig, axes = mpf.plot(df_plot, type='candle', mav=mav, volume=False, style=style,
                             title=title, ylabel='價格 (TWD)', figscale=1.5, returnfig=True)
        main_ax = axes[0] if isinstance(axes, list) else axes
        try:
            # 嘗試在主軸上疊加 MA（若欄位存在）
            if 'MA5' in df_plot.columns:
                main_ax.plot(df_plot.index, df_plot['MA5'], color='purple', linewidth=1.2, label='MA5')
            if 'MA10' in df_plot.columns:
                main_ax.plot(df_plot.index, df_plot['MA10'], color='darkgreen', linewidth=1.2, label='MA10')
            if 'MA20' in df_plot.columns:
                main_ax.plot(df_plot.index, df_plot['MA20'], color='gold', linewidth=1.2, label='MA20')
            try:
                main_ax.legend()
            except Exception:
                pass
        except Exception:
            # 若在這裡失敗，不影響主程式流程；僅提示
            print("⚠️ 在 mplfinance 圖上疊加 MA 線時發生錯誤（可忽略）")

    # 字體處理（盡量使用系統可用中文字）
    try:
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei', 'DejaVu Sans']
    except Exception:
        pass

    combined_chart_filename = f"{STOCK_CODE}_KLine_MA_Combined_RedUpGreenDown_90Days.png"
    save_path = OUTPUT_DIR / combined_chart_filename
    try:
        fig.savefig(save_path)
        print(f"📈 合併圖表已儲存到：{save_path}")
    except Exception as e:
        print(f"❌ 圖表儲存失敗: {e}")
    try:
        plt.show()
    except Exception:
        pass

    print("\n🎉 任務完成。")

# 執行
if __name__ == '__main__':
    main()
