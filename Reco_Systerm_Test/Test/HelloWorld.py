# -*- coding:utf-8 -*-
# Desc: This is Code Desc
print("HelloWorld")
# 实现sigmod函数的图像---逻辑回归的核心的函数
import numpy as np
import matplotlib.pyplot as plt
def sigmod(x):
    return 1/(1+np.exp(-x))
fig = plt.figure(figsize=(10, 10))
ax = fig.add_subplot(111)
X=np.linspace(-10,10,100)
ax.plot(X,sigmod(X))
ax.grid(True)
plt.show()
