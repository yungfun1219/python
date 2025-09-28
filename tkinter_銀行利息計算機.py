## 銀行利息計算機
import tkinter

# ----------------- 函式定義 -----------------
def fnOK():
    # 取得本金文字方塊的資料，型別為 Int
    vMoney = money.get() 
    # 取得年利率文字方塊的資料，型別為 Double，並除以 100 得到比率
    vRate = rate.get() / 100 
    # 計算利息 (本金 * 利率)
    interest = vMoney * vRate 
    # 將結果顯示在 lblResult 標籤上
    lblResult['text'] = '利息是 {0:.2f} 元'.format(interest) # 格式化輸出到小數點後兩位

# ----------------- 視窗與資料變數初始化 -----------------
win = tkinter.Tk()
win.title('銀行利息計算')
win.geometry('240x150')

# 指定 tkinter 套件的整數物件 (IntVar)，用於綁定本金輸入框
money = tkinter.IntVar()
# 指定 tkinter 套件的浮點數物件 (DoubleVar)，用於綁定年利率輸入框
rate = tkinter.DoubleVar()

# ----------------- 元件建立與配置：本金輸入 -----------------
# 標籤：本金
lblMoney = tkinter.Label(win, text='本金', padx=10, pady=8)
lblMoney.grid(row=0, column=0)
# 輸入框：本金 (寬度 15，變數綁定 money)
txtMoney = tkinter.Entry(win, width=15, textvariable=money) 
txtMoney.grid(row=0, column=1)

# ----------------- 元件建立與配置：年利率輸入 -----------------
# 標籤：年利率
lblRate = tkinter.Label(win, text='年利率', padx=10, pady=8)
lblRate.grid(row=1, column=0)
# 輸入框：年利率 (寬度 15，變數綁定 rate)
txtRate = tkinter.Entry(win, width=15, textvariable=rate) 
txtRate.grid(row=1, column=1)

# ----------------- 元件建立與配置：按鈕與結果顯示 -----------------
# 按鈕：確定 (點擊時執行 fnOK 函式)
btnOk = tkinter.Button(win, text='確定', command=fnOK)
btnOk.grid(row=2, column=0)
# 標籤：利息結果 (初始文字為空)
lblResult = tkinter.Label(win, text='', padx=10, pady=8)
lblResult.grid(row=2, column=1)

# ----------------- 啟動主迴圈 -----------------
win.mainloop()