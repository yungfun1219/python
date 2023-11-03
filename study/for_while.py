""" # while
a=1
sum=0
while a<=100:
   sum =sum+a
   a+=1

print("1加到%d的數量是%d"%(a,sum)) 
"""

# for
"""
for n in range(1,6):
    print(n)
"""

#for else
# for n in range(1,6):
#     if n%2==0:
#         print(n)
#     else:
#         continue
# else:
#     print("完成")

for letter in 'Runoob': 
   if letter == 'o':
      continue
    #   pass
      print ('执行 pass 块')
   print ('当前字母 :', letter)
 
print ("Good bye!")