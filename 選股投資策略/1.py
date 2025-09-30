from datetime import date
from dateutil.relativedelta import relativedelta

start_date = "2010/01/01"

        # 1. 設定開始和結束日期
        # 將字串轉換為 datetime.date 物件
year, month, day = map(int, start_date.split('/'))
current_date = date(year, month, day)
        
        # 取得今天的日期
today = date.today()

print(start_date)
print(current_date.strftime('%Y/%m/%d'))
print(today.strftime('%Y/%m/%d'))

   
print(f"【開始日期】: {current_date.strftime('%Y/%m/%d')}")
print(f"【計算至今日】: {today.strftime('%Y/%m/%d')}\n")
print("=" * 35)
print("計算結果 (每月的第一天或原始日期):")
print("=" * 35)
'''
        # 2. 迴圈進行月份遞增計算
date_list = []
count = 0
    
    # 當前日期小於或等於今天時，繼續迴圈
while current_date <= today:
        
        # 將日期加入列表
    date_list.append(current_date)
        
        # 格式化輸出，每 5 個日期換一行，方便閱讀
    print(f"{current_date.strftime('%Y/%m/%d'):<12}", end="")
    count += 1
    if count % 5 == 0:
        print() # 換行
            
        # 3. 遞增月份
        # 使用 relativedelta(months=1) 進行精確的月份遞增，
        # 它能正確處理不同月份的天數和閏年問題。
    current_date += relativedelta(months=1)
        
print("\n" + "=" * 35)
print(f"總共產生了 {len(date_list)} 個月份的日期。")

# 執行程式：從 2010/01/01 開始計算到今天

# 如果你需要使用這個日期列表進行後續分析，可以取消以下註解
print("\n完整日期列表 (Python list 格式):")
print(result_dates)

'''