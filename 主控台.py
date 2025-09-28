import sqlite3

def fnSearch(pid):
    sqlstr = "SELECT * FROM 產品 WHERE 編號='{}'".format(pid)
    cursor = cn.execute(sqlstr)
    return cursor.fetchone()

def fnCreate():
    pid = input("編號：")
    if fnSearch(pid) != None:
        print("編號:{} 重複".format(pid))
        return
    name = input("品名：")
    price = int(input("單價："))
    sqlstr = "INSERT INTO 產品 (編號, 品名, 單價) VALUES ('{}','{}',{})".format(pid, name, price);

    cn.execute(sqlstr)
    cn.commit()
    print("產品記錄新增成功")

def fnUpdate():
    pid = input("編號：")
    if fnSearch(pid) == None:
        print("無編號:{} 的產品記錄".format(pid))
        return
    name = input("品名：")
    price = int(input("單價："))
    sqlstr = "UPDATE 產品 SET 品名='{}', 單價={} WHERE 編號='{}'".format(name, price, pid);

    cn.execute(sqlstr)
    cn.commit()
    print("產品記錄修改成功")

def fnDelete():
    pid = input("編號：")
    if fnSearch(pid) == None:
        print("無編號:{} 的產品記錄".format(pid))
        return
    sqlstr = "DELETE FROM 產品 WHERE 編號='{}'".format(pid);
    cn.execute(sqlstr)
    cn.commit()
    print("產品記錄刪除成功")

def fnRead():
    sqlstr = "SELECT * FROM 產品";
    cursor = cn.execute(sqlstr)
    listProduct = list(cursor.fetchall())
    print("編號\t品名\t單價")
    print("===================")
    for row in listProduct:
        for col in row:
            print(col, end="\t")
        print()
    cursor.close()

cn = sqlite3.connect("myDb.db")
while True:
    print("==產品管理系統==")
    print("1. 新增產品")
    print("2. 修改產品")
    print("3. 刪除產品")
    print("4. 顯示產品")
    print("5. 離開系統")
    print("===================")
    n = int(input("請選擇功能:"))
    if n == 1:
        fnCreate()
    elif n == 2:
        fnUpdate()
    elif n == 3:
        fnDelete()
    elif n == 4:
        fnRead()
    elif n == 5:
        print("離開系統")
        break
    else:
        print("無此功能")
    print()

cn.close()
