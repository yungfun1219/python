import tkinter	# 匯入 tkinter
win = tkinter.Tk()	            # 執行 Tk() 函式建立視窗物件，並指定物件名稱為 win
win.title("第一個視窗應用程式")  # 指定視窗標題為「第一個視窗應用程式」
win.geometry("400x200")	        # 指定視窗寬 400px，高 200px
win.mainloop()	                # 呼叫 mainloop() 方法使視窗運作，直到關閉視窗為止


lbl = tkinter.Label(win, text="Hello")  # 在 win 視窗建立 lbl 標籤，文字顯示 "Hello"
lbl.grid(row=1, column=2)
win.mainloop()	 

