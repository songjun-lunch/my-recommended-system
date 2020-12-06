# -*- coding:utf-8 -*-
# Desc: jieba分词的理解
import jieba
#strs=["我来到北京清华大学","乒乓球拍卖完了","中国科学技术大学"]
seg_list1 = jieba.cut("我来到北京清华大学", cut_all=False)#精确模式--不能够解决奇异问题
print("full mode result is:",list(seg_list1))
#full mode result is: ['我', '来到', '北京', '清华大学']
seg_list2 = jieba.cut("我来到北京清华大学", cut_all=True)#使用全模式
print("another mode result is:",list(seg_list2))
#another mode result is: ['我', '来到', '北京', '清华', '清华大学', '华大', '大学']
seg_list = jieba.cut("他来到了网易杭研大厦")  # 默认是精确模式
print(", ".join(list(seg_list)))
#搜索引擎模式
seg_list = jieba.cut_for_search("小明硕士毕业于中国科学院计算所，后在日本京都大学深造")  # 搜索引擎模式
print(", ".join(list(seg_list)))

str2="李小福是创新办主任,也是云计算方面的专家."
seg_list3 = jieba.lcut(str2)
print("/ ".join(list(seg_list3)))
# 之前： 李小福 / 是 / 创新 / 办 / 主任 / 也 / 是 / 云 / 计算 / 方面 / 的 / 专家 /
jieba.load_userdict("./data/userDIct.txt")
seg_list4= jieba.lcut(str2)
print("/".join(list(seg_list4)))
# 加载自定义词库后：　李小福 /¡ 是 / 创新办 / 主任 / 也 / 是 / 云计算 / 方面 / 的 / 专家 /
jieba.add_word("自定义词")
jieba.add_word("得得得")
seg_list5= jieba.lcut("python是一个自定义词得得得，所以很好")
print("/".join(list(seg_list5)))



#词性标注
import jieba
import jieba.posseg as pseg
print(jieba.__version__)#0.39
words = pseg.cut("我爱北京天安门") #jieba默认模式
# jieba.enable_paddle() #启动paddle模式。 0.40版之后开始支持，早期版本不支持
# words = pseg.cut("我爱北京天安门",use_paddle=True) #paddle模式
for word, flag in words:
    print('%s %s' % (word, flag))
# ...
# 我 r
# 爱 v
# 北京 ns
# 天安门 ns