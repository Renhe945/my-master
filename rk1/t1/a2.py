# 01-第一个Python代码.py
# 02-注释.py
# 03-变量格式化输出.py
# 04-变量转换.py


# a="10"# 字符串--数字
# print(type(a))#<class 'str'>
# print(int(a))#10
# print(type(int(a)))#<class 'int'>
#
# # 数字---字符串
#
# a1 = 10
# print(str(a1))#10
# print(type(str(a1)))#<class 'str'>
#
# # 转bool
#
# print(bool("a1"))  # True
# print(bool(""))  # False
# print(bool(10))  # True
# print(bool(0))  # Flase
# print(bool(9.1))  # True
#
# # 浮点--整数
#
# print(int(1.2))
# print(int(1.6))
# print(float(1))


# print("------05-变量运算------")
# b1 = 1
# b2 = 5
# print(b1/b2)#3/6=0.5
# print(b1//b2) # 求商 0
# # 没有++ --
# b1+=1
# b2-=2
# print(b1)
# print(b2)
# # 字符串和数字 不能直接相加
# print("字符串和数字相加",str(1)+"1")
# print("字符串转换后和数字相加",1+int("1"))
# #python特殊语法
# a = 1
# b = 2
# a,b=b,a
# print("python特殊语法",a,b)

# 06-分支语句.py
# input进来得内容都是字符串
# age=int(input("input get：\n"))
# if age >=18:
#     print("成年")
# else:
#     print("未成年")
#
# week=input("请输入周几：\n")
# if week == "一":
#     print("学习 数学")
# elif week == "二":
#     print("学习 语文")
# elif week == "三":
#     print("学习 英文")
# else:
#     print("已经毕业了  哈哈哈哈！！！")

# 07-分支语句嵌套.py
# print("-----------07-分支语句嵌套----------")
# 银行取钱
# a7 = "123456"   #卡号
# b7 = 123        #密码
# price7 = 100    #余额
# ab7 = input("请输入卡号: \n")
# if ab7 == a7:
#     print("卡号正确！")
#     pwd = int(input("请输入密码: \n"))
#     if pwd == b7:
#         print("密码正确！")
#         money = float(input("请输入要取的金额: \n"))
#         if (price7 > money):
#             print("取了{}元，剩余{} 元".format(money,price7 - money))
#         else:
#             print("余额不足")
#     else:
#          print("密码不正确")
# else:
#     print("卡号不正确")

# print("----------------08-课堂练习------------- \n")
# import random
# print(random.randint(2,4)) # 包含[1,3]
# 石头1 剪刀2 布3

# 需求： 电脑  自己
# 自己输入input，电脑靠随机数
# 打印自己赢得情况，平局情况，电脑赢得情况
# while True:
#     a8= int(input("请输入石头1，剪刀2，布3 \n"))
#     b8=random.randint(1,3)#随机生成一个 1 到 3 之间的整数
#     c8 = {1: '石头', 2: '剪刀', 3: '布'}#义了一个字典
#     print(f"你选择了：{c8[a8]}")
#     print(f"电脑选择了：{c8[b8]}")
#     if(a8 ==1 and b8==2)or(a8==2 and b8==3)or(a8==3 and b8==1):
#         print("玩家赢")
#     elif a8==b8:
#         print("平局")
#     else:
#         print("电脑赢")
#     b8 = input("是否继续游戏？（y/n）")
#     if b8.lower() != 'y':
#         break#跳出循环

# 09-循环while.py
#使用 while 循环打印从 0 到 4 的数字。只要 count 小于 5，循环就会继续执行。
# count =0
# while count<5:
#     print(count)
#     count+=1

a9=0
i9=1
while i9<=100:
    a9+=i9
    i9+=1
    print(i9,"+",a9,"==",a9)
    print("查看当前a9是什么", a9)
    print("查看当前i9是什么",i9)
    print("最终结果----------------------------",a9)


# 10-循环for.py


# 11-for和else.py


# 12-高级变量_列表.py


# 13-高级变量_元组.py


# 14-高级变量_字典.py


# 15-高级变量_集合.py


# 16-可变数据类型和不可变数据类型.py


# 17-学生管理系统.py


# 01 - 字符串.py

# 02 - 函数.py

# 03 - 函数返回值和局部变量和全局变量.py

# 04 - 匿名函数.py

# 05 - 面向对象.py

# 06 - 面向对象初始化方法.py

# 07 - 面向对象继承.py

# 08 - 面向对象重写.py


# 09 - 面向对象多继承.py

# 10 - 面向对象多继承菱形问题.py

# 11 - 面向对象继承私有属性和私有方法.py

# 12 - 面向对象类属性.py






































































