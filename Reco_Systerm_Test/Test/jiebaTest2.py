# -*- coding:utf-8 -*-
# Desc: This is Code Desc
# -*- coding: utf-8 -*-
# https://github.com/fxsjy/jieba
import os

print(os.getcwd())
print(os.path.dirname(os.getcwd()))
print(os.path.dirname(os.path.dirname(os.getcwd())))
# encoding=utf-8
import jieba

# 精确模式，试图将句子最精确地切开，适合文本分析；
# 全模式，把句子中所有的可以成词的词语都扫描出来, 速度非常快，但是不能解决歧义；
# 搜索引擎模式，在精确模式的基础上，对长词再次切分，提高召回率，适合用于搜索引擎分词。
# jieba.cut 方法接受三个输入参数: 需要分词的字符串；cut_all 参数用来控制是否采用全模式；HMM 参数用来控制是否使用 HMM 模型
# jieba.cut_for_search 方法接受两个参数：需要分词的字符串；是否使用 HMM 模型。该方法适合用于搜索引擎构建倒排索引的分词，粒度比较细
# 待分词的字符串可以是 unicode 或 UTF-8 字符串、GBK 字符串。注意：不建议直接输入 GBK 字符串，可能无法预料地错误解码成 UTF-8
# jieba.cut 以及 jieba.cut_for_search 返回的结构都是一个可迭代的 generator，可以使用 for 循环来获得分词后得到的每一个词语(unicode)，或者用
# jieba.lcut 以及 jieba.lcut_for_search 直接返回 list
# jieba.Tokenizer(dictionary=DEFAULT_DICT) 新建自定义分词器，可用于同时使用不同词典。jieba.dt 为默认分词器，所有全局分词相关函数都是该分词器的映射。
seg_list = jieba.lcut("我来到北京清华大学", cut_all=True)
print("Full Mode: " + "/ ".join(seg_list))  # 全模式
print(type(seg_list))

seg_list = jieba.cut("我来到北京清华大学", cut_all=False)
print("Default Mode: " + "/ ".join(seg_list))  # 精确模式
print(type(seg_list))

seg_list = jieba.cut("他来到了网易杭研大厦")  # 默认是精确模式
print(", ".join(seg_list))

seg_list = jieba.cut_for_search("小明硕士毕业于中国科学院计算所，后在日本京都大学深造")  # 搜索引擎模式
print(", ".join(seg_list))

print("jieba analysis result is:")

from jieba import analyse

# 引入TextRank关键词抽取接口
textrank = analyse.textrank  # Extract keywords from sentence using TextRank algorithm

# 原始文本
text = "线程是程序执行时的最小单位，它是进程的一个执行流，\
        是CPU调度和分派的基本单位，一个进程可以由很多个线程组成，\
        线程间共享进程的所有资源，每个线程有自己的堆栈和局部变量。\
        线程由CPU独立调度执行，在多CPU环境下就允许多个线程同时运行。\
        同样多线程也可以实现并发操作，每个请求分配一个线程来处理。"

# 基于TextRank算法进行关键词抽取
keywords = textrank(text, withWeight=True)
# 输出抽取出的关键词
for keyword, weight in keywords:
    print(keyword + "/" + str(weight))
# 基本思想:
# 将待抽取™关键词的文本进行分词
# 以固定窗口大小(默认为5，通过span属性调整)，词之间的共现关系，构建图
# 计算图中节点的PageRank，注意是无向带权图
s = "此外，公司拟对全资子公司吉林欧亚置业有限公司增资4.3亿元，增资后，吉林欧亚置业注册资本由7000万元增加到5亿元。吉林欧亚置业主要经营范围为房地产开发及百货零售等业务。目前在建吉林欧亚城市商业综合体项目。2013年，实现营业收入0万元，实现净利润-139.13万元。"
for x, w in jieba.analyse.textrank(s, withWeight=True):
    print('%s %s' % (x, w))

print("jieba tfidf:")
import jieba.analyse

for x, w in jieba.analyse.extract_tags(s, topK=5, withWeight=True, allowPOS=()):
    print('%s %s' % (x, w))
# sentence 为待提取的文本
# topK 为返回几个 TF/IDF 权重最大的关键词，默认值为 20
# withWeight 为是否一并返回关键词权重值，默认值为 False
# allowPOS 仅包括指定词性的词，默认值为空，即不筛选
# jieba.analyse.TFIDF(idf_path=None) #新建 TFIDF 实例，idf_path 为 IDF 频率文件
# jieba tfidf:
# 欧亚 0.7300142700289363
# 吉林 0.659038184373617
# 置业 0.4887134522112766
# 万元 0.3392722481859574
# 增资 0.33582401985234045