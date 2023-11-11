# 定義裝飾器工廠: 可接收額外的參數
# def myFactory():
#     def myDeco(cb):
#         def run():
#             print("裝飾器內部的程式")
#             cb()
#         return run
#     return myDeco

# # 使用裝飾器
# @myFactory()
# def test():
#     print("普通函式的程式")
    
# test()

"""
def myFactory(base):
    def myDeco(cb):
        def run():
            print("裝飾器內部的程式",base)
            result=base*2
            cb(result)
        return run
    return myDeco

# 使用裝置器
@myFactory(3)
def test(result):
    print("普通函式的程式",result)
    
test()
"""
def calculate(callback):
    def run():
        total=0
        for n in range(10):
            total+=n
        callback(total)
    return run


# 使用裝置器
@myFactory(3)
def test(result):
    print("普通函式的程式",result)
    
test()
