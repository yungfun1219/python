# 台股分析工具 (Taiwan Stock Analyzer)

這個專案提供台灣股市資料的抓取和分析工具。

## 功能說明

### 三大法人買賣超日報 (3ii_Net_buy_sell.py)
- 從台灣證券交易所抓取每日三大法人買賣超資料
- 自動建立資料儲存目錄結構
- 將資料儲存為 CSV 格式
- 支援日期格式: YYYYMMDD

## 目錄結構
```
Jason_Stock_Analyzer/
│
├── 3ii_Net_buy_sell.py    # 三大法人買賣超資料抓取
├── Datas/                 # 資料存放目錄
│   └── raw/              # 原始資料
│       └── 3ii_Net_buy_sell/  # 三大法人買賣超資料
│
└── README.md             # 本說明檔案
```

## 使用方法

### 三大法人買賣超日報
```python
from datetime import date

# 使用今天日期
today = date.today()
target_date = today.strftime("%Y%m%d")

# 或指定特定日期
target_date = "20251008"  # 格式：YYYYMMDD

# 抓取資料
df_data = fetch_twse_t86_data(target_date)
```

## 注意事項
1. 需要安裝以下套件：
   - requests
   - pandas
   - urllib3
2. 資料來源為台灣證券交易所，非交易日無資料
3. 建議使用 Python 3.6 以上版本

## 錯誤處理
- HTTP 錯誤：通常表示該日期無交易資料
- 連線錯誤：檢查網路連線狀態
- 其他錯誤：請查看錯誤訊息詳細說明

## 授權
MIT License