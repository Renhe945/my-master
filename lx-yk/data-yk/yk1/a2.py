import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression,LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score,confusion_matrix,mean_squared_error,recall_score,roc_auc_score
from sklearn.model_selection import GridSearchCV
import joblib
import matplotlib.pyplot as plt
# 让图片中可以显示中文
plt.rcParams['font.sans-serif'] = 'SimHei'
# 让图片中可以显示负号
plt.rcParams['axes.unicode_minus'] = False
#%%
# 1、读取数据并查看前5行以及数据均值、中位数等统计量。（2分）
df = pd.read_excel("E:///python代码/lx-yk/data-yk/yk1/客户信息.xlsx")
df.mean()
df.median()
#%% 2、EDA分析：可视化展示并形成分析结论（5分）
plt.scatter(df["年龄(岁)"],df["收入(万元)"])
plt.show()

# 结论：随着年纪增长，收入逐渐增多

X = df[['年龄(岁)']]
y = df['收入(万元)']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train
#%% 3、数据建模：选择合适的模型建模训练，并选用合适的指标评估模型。（8分）
lr = LinearRegression()
lr.fit(X_train,y_train)
y_pred = lr.predict(X_test)
y_pred
mean_squared_error(y_test,y_pred)
#%%4、建模效果可视化展示（5分）
plt.scatter(X_test,y_test,color='red',label='真实值')
plt.scatter(X_test,y_pred,color='green',label='测试值')
plt.legend()
plt.show()
# df["label"] = df["收入(万元)"].map(lambda  x: '高净值' if x >= 50 else '低净值')
# df


#%%5、使用模型对样本分群，对样本每条数据打上分群后的标签（如高净值、低净值）（5分）
X = df[["年龄(岁)","收入(万元)"]]
# 注意
kmeans = KMeans(n_clusters=2, random_state=0)
kmeans.fit(X)
# 获取簇的标签
labels = kmeans.labels_
print(labels)
labels# # 假设我们将簇0视为“低净值”，簇1视为“高净值”
df['cluster_label'] = ['低净值' if label == 0 else '高净值' for label in labels]
# 查看结果
print(df)








