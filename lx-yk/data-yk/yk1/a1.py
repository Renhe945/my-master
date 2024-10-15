#%%
# 题目三 、银行客户分群（25分）。
import pandas as pd
import numpy as np
# 利用客户信息.xlsx数据集，选择合适模型，解决银行客户分群问题。
# 1、读取数据并查看前5行以及数据均值、中位数等统计量。（2分）
a1=pd.read_excel("E:///python代码/lx-yk/data-yk/yk1/客户信息.xlsx")
print(a1.head(5))
print("统计信息。\n",a1.describe())
print("数据均值。\n",a1.mean())
print("中位数。\n",a1.median())
#%%

# 2、EDA分析：可视化展示并形成分析结论（5分）
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['Simhei']
plt.rcParams['axes.unicode_minus'] = True
plt.scatter(a1["年龄(岁)"],a1["收入(万元)"])
plt.title("年龄与收入关系散点图")
plt.xlabel("年龄(岁)")
plt.ylabel("收入(万元)")
plt.show()
# 结论：随着年纪增长，收入逐渐增多
#%%
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split

# 3、数据建模：选择合适的模型建模训练，并选用合适的指标评估模型。（8分）
X = a1[['年龄(岁)']]
y = a1['收入(万元)']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

kmeans = KMeans(n_clusters=3, random_state=42)
kmeans.fit(X_train,y_train)
# 获取聚类标签
labels = kmeans.labels_
labels
#%%
from sklearn.metrics import mean_squared_error

# 4、建模效果可视化展示（5分）

from sklearn.metrics import mean_squared_error
y_pred = kmeans.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"均方误差：{mse}")

plt.figure(figsize=(10, 6))
plt.scatter(X_test, y_test, color='red', label='真实值')
plt.scatter(X_test, y_pred, color='green', label='预测值')
plt.title("真实值与预测值对比图")
plt.xlabel("年龄(岁)")
plt.ylabel("收入(万元)")
plt.legend()
plt.show()

#%%
# 5、使用模型对样本分群，对样本每条数据打上分群后的标签（如 高净值、低净值）（5分）
a1['分'] = kmeans.predict(a1[['年龄(岁)']])
# 使用列表推导式将数字标签转换为文字标签
a1['分类'] = ['低净值' if i == 0 else '高净值' for i in a1['分']]

print(a1.head())