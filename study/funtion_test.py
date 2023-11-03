def f(a,b,*,c):
    return a+b+c

f(1,2,c=3)   # 报错