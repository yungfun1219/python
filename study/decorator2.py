# # 定義裝置器
# def myDeco(cb):
#     def run():
#         print("裝飾器內部的程式")
#         cb(5)
#     return run

# # 使用裝置器
# @myDeco
# def test(n):
#     print("普通函式的程式",n)
    
# test()

# 定義裝置器
def calculate(cb):
    def run():
        result=0
        for n in range(51):
            result+=n
        print(result)
        cb()
    return run

# 使用裝置器
@calculate
def show():
    print("普通函式的程式")
    
show()