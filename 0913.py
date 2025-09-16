# list推導式--不能巢式迴圈
# [運算式 for 指定元素名稱 in 指定列表]
from datetime import datetime
from pyparsing import line


x = [1,2,3,4]
print(x)

y = [i+1 for i in x]
print(y)

z = [i+2 for i in x if i !=1]  # if 條件式
print(z)

A = [[i,i+2] for i in x if i !=1]  # if 條件式
print(A)

#開啟檔案
# [運算式 for 指定元素名稱 in open(指定檔案名稱)]
#lines = [line for line in open("d:/Python_repo/python/text.txt","r")]
#print(lines.read())

import datetime
print(datetime.datetime.strptime()).now().strftime("%Y-%m-%d %H:%M:%S")
