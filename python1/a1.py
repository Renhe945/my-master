from itertools import count

print("1. 打印出 '你好，世界！'")
print("你好，世界！")

print("# 2. 打印出字符 '我' 的Unicode码点。")
char = "我"
print(ord(char))

print("3. 创建一个包含中文字符的字符串，然后打印出其中每个字符的Unicode码点。")
ch = "超第二天"
for char in ch:
    print(ord(char))
print("4. 创建一个包含多个中文字符的列表，然后使用循环打印出每个字符。")

list ={"超","第","二","天"}
for char in list:
    print(ord(char))
print( "5. 将Python' 和 '编程' 保存为变量，然后使用字符串拼接打印出完整的句子")
a1="Python"
a2="编程"
print(f"{a1} {a2}")

print("6. 编写一个程序，接受用户输入的中文字符串，然后将其中的每个字符按照 Unicode 码点从小到大排序，并打印出排序结果")
# 获取用户输入的中文字符串
input_string = input("请输入宁符串:")
char_list = sorted(input_string)
print("".join(char_list))
print("# 7. 使用列表推导式创建一个包含中文字符的Unicode码点列表，然后打印出列表。")
s = "中文列表推导式"
unicode_points = [ord(char) for char in s]
print(unicode_points)

print("# 8. 编写一个程序，统计一个中文字符串中每个字符出现的次数，并打印出统计结果。")
s1="我吃我的苹果统计次数"
count1={}
for char in s1:
    if char in count1:
        count1[char]+=1
    else:
        count1[char]=1
print(count1,"：次")


print("# 9. 创建一个包含中文字符的字典，将每个字符作为键，其对应的Unicode码点作为值，并打印出字典。")
s2="中文字符的字典，将每个字符作为键"
char_dict = {char: ord(char) for char in s}
print(char_dict)

print("# 10. 使用格式化字符串，将 '学习' 和 'Python' 插入到 '我正在_编程。' 中，并打印出完整句子。")

word1 = "学习"
word2 = "Python"
print(f"我正在{word1} {word2}编程。")