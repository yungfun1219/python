# vec=[2,4,6]
# a = [3*x for x in vec]
# print("3*x for x in vec結果為",end="")
# print(a)

# a = [[x,x**2] for x in vec if x>5]
# print("[x,x**2] for x in vec結果為",end="")
# print(a)

# a=range(1,10)
# b=range(1,10)
# ab=[x*y for x in a for y in b]
# print(ab)

matrix = [[1,2,3,4],[5,6,7,8],[9,10,11,12]]
m = [[ x[i] for x in matrix ] for i in range(4)]
print(m)

trans = []
for i in range(4):
    trans.append([row[i] for row in matrix])
# print(trans)

for i in range(4):
    trans_row=[]
    for row in matrix:
        trans_row.append(row[i])
    trans.append(trans_row)
print(trans)


# print([[row[i] for row in matrix] for i in range(4)])
# print(matrix1)