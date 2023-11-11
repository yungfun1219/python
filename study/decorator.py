# 定義裝置器
def myDeco(cb):
    def run():
        print("裝飾器內部的程式")
        cb()    #這個回呼函式，
    return run

# 使用裝置器
@myDeco
def test():
    print("普通函式的程式")
    
test()