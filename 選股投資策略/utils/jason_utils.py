# 民國年轉西元年
def transform_date(date):
    try:
        if '/' in date:
            Y, m, d = date.split('/')
        elif '-' in date:
            Y, m, d = date.split('-')
        else:
            raise ValueError("日期格式錯誤，無法辨識分隔符號")
    except Exception as e:
        print(f"轉換日期時發生錯誤: {e}")
        return date  # 發生錯誤時，回傳原始日期
    
    return str(int(Y) + 1911) + '/' + m + '/' + d

