import pandas as pd
import io

infile = r"D:\Python_repo\python\Jason_Stock_Project\datas\processed\get_holidays\trading_day_2021-2025.csv"
outfile = infile.replace(".csv", "_OK.csv")

encodings = ["utf-8", "utf-8-sig", "cp950", "big5", "latin1"]
read_errors = []

df = None
used_enc = None
used_sep = None

# 嘗試多種編碼並讓 pandas 嘗試自動偵測分隔符（engine='python' + sep=None）
for enc in encodings:
    try:
        # engine='python' + sep=None 會嘗試推斷分隔符
        df = pd.read_csv(infile, encoding=enc, sep=None, engine='python')
        used_enc = enc
        used_sep = "inferred"
        print(f"[INFO] 成功以編碼讀取：{enc}")
        break
    except Exception as e:
        read_errors.append((enc, str(e)))
        # 嘗試以常見分隔符逐一嘗試（逗號、分號、tab）
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(infile, encoding=enc, sep=sep, engine='python')
                used_enc = enc
                used_sep = sep
                print(f"[INFO] 成功以編碼 {enc} + sep '{sep}' 讀取")
                break
            except Exception:
                continue
        if df is not None:
            break

if df is None:
    print("[ERROR] 無法以任何常見編碼讀取檔案。嘗試過的錯誤：")
    for enc, msg in read_errors:
        print(f"  - {enc}: {msg}")
    raise SystemExit(1)

# 顯示欄位資訊供偵錯
print("\n[INFO] 欄位名稱：", list(df.columns))
print("[INFO] 檔案前 5 列：")
print(df.head(5).to_string(index=False))

# 嘗試自動找出日期欄位
candidates = []

# 先用常見欄名搜尋
lower_cols = {c: c.lower() for c in df.columns}
for c, lc in lower_cols.items():
    if any(k in lc for k in ("date", "日期", "day", "trade_day", "trading_day")):
        candidates.append(c)

# 若沒有常見名稱，再對每一欄嘗試解析，選解析成功率最高的欄
def parse_success_rate(series):
    try:
        parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)
        non_na = parsed.notna().sum()
        return non_na / max(1, len(parsed))
    except Exception:
        return 0.0

if not candidates:
    rates = {c: parse_success_rate(df[c]) for c in df.columns}
    # 只挑成功率 > 0.1 的候選欄（避免完全亂猜）
    sorted_rates = sorted(rates.items(), key=lambda x: x[1], reverse=True)
    print("\n[INFO] 各欄位解析為日期的成功率（前 10）：")
    for c, r in sorted_rates[:10]:
        print(f"  {c}: {r:.3f}")
    # 選擇成功率最高且 >= 0.2（可修改閾值）的欄位
    best_col, best_rate = sorted_rates[0]
    if best_rate >= 0.20:
        candidates = [best_col]
        print(f"[INFO] 自動選擇欄位 '{best_col}' 作為日期欄（成功率 {best_rate:.3f}）")
    else:
        # 若都太低，提示使用者並停止（不拋 KeyError）
        print("\n[ERROR] 無法自動辨識出可靠的日期欄位。請檢查上方的欄位清單與前幾列，")
        print("        並在程式中指定正確的日期欄位名稱（例如：df['你的欄位'] = pd.to_datetime(...)）。")
        raise SystemExit(2)

# 以第一個候選欄位為日期欄，嘗試解析
date_col = candidates[0]
print(f"\n[INFO] 使用 '{date_col}' 當作日期欄，開始解析...")

df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=False)

parsed_count = df[date_col].notna().sum()
total_count = len(df)
if parsed_count == 0:
    print(f"[ERROR] 欄位 '{date_col}' 解析後沒有有效日期（全為 NaT）。請改用其他欄位。")
    raise SystemExit(3)

print(f"[INFO] 成功解析日期數量：{parsed_count}/{total_count}")

# 過濾 >= 2025-12-01
cutoff = pd.Timestamp("2025-11-15")
df_filtered = df[df[date_col] >= cutoff].copy()

# 若沒有任何列符合，提醒並輸出空檔（視需求可改）
print(f"[INFO] 篩選後列數：{len(df_filtered)} (保留日期 >= {cutoff.date()})")

# 做些後處理：排序、去重
df_filtered = df_filtered.sort_values(by=date_col).drop_duplicates(subset=[date_col])

# 另存成 UTF-8-sig
df_filtered.to_csv(outfile, index=False, encoding="utf-8-sig")
print(f"[DONE] 輸出檔案：{outfile} (encoding=utf-8-sig)")
