# -*- coding:utf-8 -*-
# Desc: This is Code Desc
# 1-准备Spark的环境
import os

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 如果有多个版本的python需要指定，如果不指定可能会报错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from pyspark.sql import SparkSession
from pyspark.ml.feature import  Tokenizer,HashingTF,IDF,CountVectorizer
spark = SparkSession.builder.master("local[*]").appName("tfidf").getOrCreate()
# 2-准备数据
data=spark.createDataFrame( [(0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")],["label","sentence"])
data.show(truncate=False)
# +-----+-----------------------------------+
# |label|sentence                           |
# +-----+-----------------------------------+
# |0.0  |Hi I heard about Spark             |
# |0.0  |I wish Java could use case classes |
# |1.0  |Logistic regression models are neat|
# +-----+-----------------------------------+
# 分词
tokenizer = Tokenizer(inputCol="sentence", outputCol="features")
wordsData = tokenizer.transform(data)
wordsData.show(truncate=False)
# +-----+-----------------------------------+------------------------------------------+
# |label|sentence                           |features                                  |
# +-----+-----------------------------------+------------------------------------------+
# |0.0  |Hi I heard about Spark             |[hi, i, heard, about, spark]              |
# |0.0  |I wish Java could use case classes |[i, wish, java, could, use, case, classes]|
# |1.0  |Logistic regression models are neat|[logistic, regression, models, are, neat] |
# +-----+-----------------------------------+------------------------------------------+
# 3-引出TF
countVec = CountVectorizer(inputCol="features", outputCol="rawFeatures")
countModel = countVec.fit(wordsData)
featuresedData = countModel.transform(wordsData)
featuresedData.show(truncate=False)
# 4-引出IDF
idf = IDF(inputCol="rawFeatures", outputCol="featuresIDF")
iidfModel = idf.fit(featuresedData)
rescaledData = iidfModel.transform(featuresedData)
# 5-计算TFIDF得预测值
rescaledData.select("label","featuresIDF").show(truncate=False)
# +-----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |label|featuresIDF                                                                                                                                                                                     |
# +-----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |0.0  |(262144,[24417,49304,73197,91137,234657],[0.28768207245178085,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453])                                                     |
# |0.0  |(262144,[20719,24417,55551,116873,147765,162369,192310],[0.6931471805599453,0.28768207245178085,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453])|
# |1.0  |(262144,[13671,91006,132713,167122,190884],[0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453])                                                    |
# +-----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
