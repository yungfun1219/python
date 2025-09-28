import tkinter	
win = tkinter.Tk()	
win.title("熱門動漫人物")	
win.geometry("350x90")	
lbl00 = tkinter.Label(win, text='火焰忍者-鳴人', bg='yellow')	
lbl03 = tkinter.Label(win, text='航海王-魯夫', bg='yellow')	
lbl12 = tkinter.Label(win, text='天兵公園-鳥哥', bg='yellow')	
lbl21 = tkinter.Label(win, text='櫻桃小丸子-小丸子', bg='yellow')	
lbl00.grid(row=0, column=0)	# '火焰忍者-鳴人'，放置於 第 0 行 第 0 列
lbl03.grid(row=0, column=3)	# '航海王-魯夫'，放置於 第 0 行 第 3 列
lbl12.grid(row=1, column=2)	# '天兵公園-鳥哥'，放置於 第 1 行 第 2 列
lbl21.grid(row=2, column=1)	# '櫻桃小丸子-小丸子'，放置於 第 2 行 第 1 列
win.mainloop()