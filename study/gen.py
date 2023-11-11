# 建立生成器的函式
"""
def test():
    print("階段一")
    yield 5
    print("階段二")
    print("階段三")
    yield 10
# 呼叫並回傳生成器
gen=test()
print(gen)

# 搭配for迴圈中使用
for data in gen:
    print(data)
"""
def generatEven(maxNumber):
    number = 0
    while number<=maxNumber:
    # while number True:     #產生無限多       
        yield number
        number +=2
    # number=0
    # yield number
    # number+=2
    # yield number
    # number+=2
    # yield number
    
evenGenerator = generatEven(10)
for data in evenGenerator:
    print(data)
    
