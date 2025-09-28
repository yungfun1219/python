import tkinter

# 定義 fnHello 函式：按下 Hello 按鈕時執行
def fnHello():
    # 透過字典方式修改 lblShow 標籤的 text 屬性
    lblShow["text"] = "Hello World!"

# 定義 fnClear 函式：按下 Clear 按鈕時執行
def fnClear():
    # 清空 lblShow 標籤的 text 屬性
    lblShow["text"] = ""

# 建立主視窗
win = tkinter.Tk()
win.title("按鈕範例")
win.geometry("350x100")

# 建立 Hello 按鈕並綁定 fnHello 函式
btnHello = tkinter.Button(win, text="Hello", command=fnHello, padx=20)
btnHello.grid(row=0, column=0) # 放置在第 0 行 第 0 列

# 建立 Clear 按鈕並綁定 fnClear 函式
btnClear = tkinter.Button(win, text="Clear", command=fnClear, padx=20)
btnClear.grid(row=0, column=1) # 放置在第 0 行 第 1 列

# 建立顯示標籤 (lblShow)，用於顯示結果
lblShow = tkinter.Label(win, text="", font=("細明體", 18), padx=20, pady=10)
lblShow.grid(row=0, column=2) # 放置在第 0 行 第 2 列

# 啟動視窗主迴圈
win.mainloop()