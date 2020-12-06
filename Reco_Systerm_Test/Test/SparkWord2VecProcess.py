# -*- coding:utf-8 -*-
# Desc: This is Code Desc
# 1-准备Spark的环境
import os

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 如果有多个版本的python需要指定，如果不指定可能会报错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec

spark = SparkSession.builder.master("local[*]").appName("tfidf").getOrCreate()
data = spark.createDataFrame(
    [("Hi I heard about Spark".split(" "),),
     ("I wish Java could use case classes".split(" "),),
     ("Logistic regression models are neat".split(" "),)], ["text"])
data.show(truncate=False)
# word2vec
vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="features")
vecModel = vec.fit(data)
# 预测
result = vecModel.transform(data)
result.show(truncate=False)
# +------------------------------------------+------------------------------------------------------------------+
# |text                                      |features                                                          |
# +------------------------------------------+------------------------------------------------------------------+
# |[Hi, I, heard, about, Spark]              |[-0.04582532420754433,-0.051227146666497,-0.031036147475242616]   |
# |[I, wish, Java, could, use, case, classes]|[0.009998133040166326,-0.008963470347225666,-0.023745902947017124]|
# |[Logistic, regression, models, are, neat] |[0.012606503441929817,0.011686521396040917,0.04363004937767983]   |
# +------------------------------------------+------------------------------------------------------------------+
synonyms = vecModel.findSynonyms('wish', 5)
print(synonyms.collect())
# [Row(word='case', similarity=0.7668559551239014), Row(word='neat', similarity=0.7212052941322327), Row(word='Logistic', similarity=0.5371538400650024), Row(word='models', similarity=0.5201331973075867), Row(word='classes', similarity=0.16931432485580444)]
# for word, similarity in synonyms:
#     print("{}: {}".format(word, similarity))
# 如何打印每个word的词向量的表示？
vecModel.getVectors().show(truncate=False)
# +----------+-----------------------------------------------------------------+
# |word      |vector                                                           |
# +----------+-----------------------------------------------------------------+
# |heard     |[-0.04964592680335045,0.09677628427743912,0.12538667023181915]   |
# |are       |[-0.11615744978189468,-0.02255220338702202,0.12064642459154129]  |
# |neat      |[0.10850624740123749,-0.08774396032094955,0.015122845768928528]  |
# |classes   |[0.13256068527698517,-0.09345054626464844,0.02452482469379902]   |

# from pyspark.ml.feature import BucketedRandomProjectionLSH
# brp=BucketedRandomProjectionLSH(inputCol="articleVector",outputCol="hashed",numHashTables=4,bucketLength=10)
# model = brp.fit()
# model.approxSimilarityJoin(df2, Vectors.dense([1.0, 2.0]), 1).collect()