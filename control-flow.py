"""
x = int(input("請輸入一個正整數:"))
# print(type(x))
if x<0 :
    print("小於0")
elif x == 0:
    print("等於0")
elif x > 0:
    print("數值正確")
# else:
#     print("OK")

print("************")
print(x)

# for 迴圈
word = ["cat","window","defenestrate"]
for w in word:
    print(w)

"""

users = {"Hans":"active","Eleonore":"inactive","景太郎":"active"}
for user, status in users.copy().items():
    if status == "inactive":
        del users[user]

active_users = {}
for user, status in users.items():
    if status == "active":
        active_users[user] = status

print(users)