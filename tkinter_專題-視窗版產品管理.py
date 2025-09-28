import tkinter
import sqlite3

# -------------------- 輔助函式：查詢產品編號是否存在 --------------------
def fnSearch(pid):
    # 執行 SQL 查詢，尋找指定編號的產品
    sqlstr = "SELECT * FROM 產品 WHERE 編號='{}'".format(pid)
    cursor = cn.execute(sqlstr)
    # 傳回查詢結果 (若無則為 None)
    return cursor.fetchone()

# -------------------- 輔助函式：清除輸入欄位 --------------------
def fnCls():
    # 將所有資料綁定變數設為空字串或 0
    vId.set("")
    vName.set("")
    vPrice.set(0)

# -------------------- 函式：新增產品 (Create) --------------------
def fnCreate():
    pid = vId.get()
    # 檢查編號是否重複
    if fnSearch(pid) != None:
        lblMsg["text"] = "編號：{} 重複".format(pid)
        return
    name = vName.get()
    price = vPrice.get()
    # 執行 SQL INSERT 語句
    sqlstr = "INSERT INTO 產品(編號,品名,單價)VALUES('{}','{}',{})".format(pid, name, price)
    cn.execute(sqlstr)
    cn.commit() # 提交資料庫異動
    lblMsg["text"] = "產品記錄新增成功"
    fnCls() # 清除輸入欄位

# -------------------- 函式：修改產品 (Update) --------------------
def fnUpdate():
    pid = vId.get()
    # 檢查編號是否存在
    if fnSearch(pid) == None:
        lblMsg["text"] = "編號：{} 的產品記錄不存在".format(pid)
        return
    name = vName.get()
    price = vPrice.get()
    # 執行 SQL UPDATE 語句
    sqlstr = "UPDATE 產品 SET 品名='{}', 單價={} WHERE 編號='{}'".format(name, price, pid)
    cn.execute(sqlstr)
    cn.commit()
    lblMsg["text"] = "產品記錄修改成功"
    fnCls()

# -------------------- 函式：刪除產品 (Delete) --------------------
def fnDelete():
    pid = vId.get()
    # 檢查編號是否存在
    if fnSearch(pid) == None:
        lblMsg["text"] = "無編號：{} 的產品記錄".format(pid)
        return
    # 執行 SQL DELETE 語句
    sqlstr = "DELETE FROM 產品 WHERE 編號='{}'".format(pid)
    cn.execute(sqlstr)
    cn.commit()
    lblMsg["text"] = "產品記錄刪除成功"
    fnCls()

# -------------------- 函式：讀取產品列表 (Read) --------------------
def fnRead():
    # 查詢所有產品記錄
    sqlstr = "SELECT * FROM 產品"
    cursor = cn.execute(sqlstr)
    listProduct = list(cursor.fetchall()) # 取得所有記錄
    
    # 建立一個新的子視窗 (winProduct) 來顯示產品清單
    winProduct = tkinter.Tk()
    winProduct.title('產品記錄')
    winProduct.geometry('270x160')
    
    # 格式化輸出字串
    strShow = "編號\t品名\t單價\n" # 標題列
    strShow += "================================\n"
    
    # 遍歷產品列表，將每筆記錄格式化成字串
    for row in listProduct:
        for col in row:
            strShow += str(col) + "\t"
        strShow += "\n"
    
    # 建立標籤來顯示清單內容
    lblShow = tkinter.Label(winProduct, text=strShow, padx=10, pady=10)
    lblShow.grid(row=0, column=0)
    cursor.close()
    winProduct.mainloop() # 啟動子視窗主迴圈

# -------------------- 主程式設定 --------------------

# 連接資料庫 (如果不存在會自動建立)
cn = sqlite3.connect("D:\Python_repo\python\Stock_day_data\stock.db")
 
# 建立主視窗
winMain = tkinter.Tk()
winMain.title('產品管理系統')
winMain.geometry('270x160')

# -------------------- 資料綁定變數 (StringVar, IntVar) --------------------
vId = tkinter.StringVar()       # 綁定產品編號輸入框
vName = tkinter.StringVar()     # 綁定產品品名輸入框
vPrice = tkinter.IntVar()       # 綁定產品單價輸入框

# -------------------- 元件建立：標籤 (Label) --------------------
lblId = tkinter.Label(winMain, text='編號', padx=10, pady=10)
lblId.grid(row=0, column=0)
lblName = tkinter.Label(winMain, text='品名', padx=10, pady=10)
lblName.grid(row=1, column=0)
lblPrice = tkinter.Label(winMain, text='單價', padx=10, pady=10)
lblPrice.grid(row=2, column=0)
lblMsg = tkinter.Label(winMain, text='！', padx=10, pady=10) # 用於顯示操作訊息
lblMsg.grid(row=3, column=1)

# -------------------- 元件建立：輸入框 (Entry) --------------------
txtId = tkinter.Entry(winMain, width=15, textvariable=vId)
txtId.grid(row=0, column=1)
txtName = tkinter.Entry(winMain, width=15, textvariable=vName)
txtName.grid(row=1, column=1)
txtPrice = tkinter.Entry(winMain, width=15, textvariable=vPrice)
txtPrice.grid(row=2, column=1)

# -------------------- 元件建立：按鈕 (Button) --------------------
# 新增按鈕，點擊時執行 fnCreate
btnCreate = tkinter.Button(winMain, text='產品新增', command=fnCreate)
btnCreate.grid(row=0, column=2)
# 修改按鈕，點擊時執行 fnUpdate
btnUpdate = tkinter.Button(winMain, text='產品修改', command=fnUpdate)
btnUpdate.grid(row=1, column=2)
# 刪除按鈕，點擊時執行 fnDelete
btnDelete = tkinter.Button(winMain, text='產品刪除', command=fnDelete)
btnDelete.grid(row=2, column=2)
# 列表按鈕，點擊時執行 fnRead
btnRead = tkinter.Button(winMain, text='產品列表', command=fnRead)
btnRead.grid(row=3, column=2)

# -------------------- 啟動主迴圈 --------------------
winMain.mainloop()