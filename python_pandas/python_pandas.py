#s="abcde"

#for i in range(len(s)):
#    print(f"字串s的第{i}個字元是{s[i]}") 

#for i,j in enumerate(s):
#    print(f"字串s的第{i}個字元是{j}")


a=[1,2,3]
b=[4,5,6]
print([i*j for i,j in zip(a,b)])

c = {'name': 'John', 'age': 30, 'city': 'New York'}

print(c.get("ag")) 

aa=30
print("aa<15") if aa<15 else print("15=<aa<30") if aa<15 else print("aa>=30") 

