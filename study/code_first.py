# # my_list = ['google', 'runoob', 'taobao']
# # for n in my_list:
# #     print(n)
# # print(my_list[0]) # 输出 "google"
# # print(my_list[1]) # 输出 "runoob"
# # print(my_list[2]) # 输出 "taobao"

# # a, b = 0, 1
# # while b < 100:
# #     print(b,end="，")
# #     # a, b = b, a+b  ##可用下列四行指令代表
# #     n=b
# #     m=a+b
# #     a=n
# #     b=m


# # names = ['Bob','Tom','alice','Jerry','Wendy','Smith']
# # newNames = [name.upper()for name in names if len(name)>3]
# # print(newNames)

# # multiples=[i for i in range(31) if i%3==0]
# # print(multiples)

# # listdemo = ['Google','Runoob', 'Taobao']
# # newDict = {key:len(key) for key in listdemo}
# # print(newDict)

# # dict1={x:x**2 for x in (2,4,6)}
# # print(dict1)
# # print(type(dict1))

# #集合推導式
# # setnew = {i**2 for i in (1,2,3)}
# # print(setnew)
# # print(type(setnew))

# # a={x for x in "abcdefgh" if x not in "abcde"}
# # print(a)
# # print(type(a))

# #元組推導式(生成器表達式)
# # a = ( x for x in range(1,10))
# # print(a)
# # print(tuple(a))

# # #疊代器
# # list=[1,2,3,4]
# # it = iter(list)
# # for n in it:
# #     print(n,end=",")

# # class MyNumbers:
# #   def __iter__(self):
# #     self.a = 1
# #     return self
 
# #   def __next__(self):


#     x = self.a
#     self.a += 1
#     return x
 
# myclass = MyNumbers()
# myiter = iter(myclass)
 
# print(next(myiter))
# print(next(myiter))
# print(next(myiter))
# print(next(myiter))
# print(next(myiter))