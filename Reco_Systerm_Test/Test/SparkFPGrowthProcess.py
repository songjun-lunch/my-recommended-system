# -*- coding:utf-8 -*-
# Desc: 使用FPGrowth算法得到频繁项集和关联规则===推荐系统的核心的算法
# 1-准备环境
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 如果有多个版本的python需要指定，如果不指定可能会报错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
conf = SparkConf().setMaster("local[*]").setAppName("SparkALSTest")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
# 2-准备数据---事务和项集构成的数据集
df = spark.createDataFrame([(0, [1, 2, 5]), (1, [1, 2, 3, 5]), (2, [1, 2])], ["id", "items"])
# 3-数据的基本的信息的查看
df.printSchema()
df.show(truncate=False)
# +---+------------+
# |id |items       |
# +---+------------+
# |0  |[1, 2, 5]   |
# |1  |[1, 2, 3, 5]|
# |2  |[1, 2]      |
# +---+------------+
# 4-特征工程
# 5-准备FPGrowth算法
# minSupport=0.3,最小的支持度，项的个数为3，支持度计数0.9
# minConfidence=0.8 最小的置信度
fpGrowth = FPGrowth(minSupport=0.5, minConfidence=0.6, itemsCol="items",
                    predictionCol="prediction")
# 6-训练FPGrowth模型
model = fpGrowth.fit(df)
# +---------+----+
# |    items|freq|
# +---------+----+
# |      [5]|   2|
# |   [5, 1]|   2|
# |[5, 1, 2]|   2|
# |   [5, 2]|   2|
# |      [2]|   3|
# |      [1]|   3|
# |   [1, 2]|   3|
# +---------+----+
# 7-模型预测分析
model.freqItemsets.show()
# +----------+----------+------------------+
# |antecedent|consequent|        confidence|
# +----------+----------+------------------+
# |       [5]|       [1]|               1.0|
# |       [5]|       [2]|               1.0|
# |    [1, 2]|       [5]|0.6666666666666666|
# |    [5, 2]|       [1]|               1.0|
# |    [5, 1]|       [2]|               1.0|
# |       [2]|       [5]|0.6666666666666666|
# |       [2]|       [1]|               1.0|
# |       [1]|       [5]|0.6666666666666666|
# |       [1]|       [2]|               1.0|
# +----------+----------+------------------+
model.associationRules.show()
# 8-模型保存
model.transform(df).show()
# +---+------------+----------+
# | id|       items|prediction|
# +---+------------+----------+
# |  0|   [1, 2, 5]|        []|
# |  1|[1, 2, 3, 5]|        []|
# |  2|      [1, 2]|       [5]|
# +---+------------+----------+
# 如果数据集中的项出现在规则的前件中，那么我们就将规则的后腱作为transform的预测结果
