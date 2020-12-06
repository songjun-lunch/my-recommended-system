# -*- coding:utf-8 -*-
# Desc: 使用逻辑回归解决Iris数据集的处理的问题
# 1- 准备环境
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import  LogisticRegression
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import  MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("irisTest").master("local[*]").getOrCreate()
# 2- 准备数据
# 数据集的基本的属性的名称--花瓣的长度和宽度--花萼的长度和宽度
# 3- 对数据进行解析
df = spark.read.format("csv").option("header", "true").load("./data/iris.csv")
# 4- 对解析后的数据进行数据的查看
df.printSchema()
# root
#  |-- sepal_length: string (nullable = true)
#  |-- sepal_width: string (nullable = true)
#  |-- petal_length: string (nullable = true)
#  |-- petal_width: string (nullable = true)
#  |-- class: string (nullable = true)
df.show(2)
# +------------+-----------+------------+-----------+-----------+
# |sepal_length|sepal_width|petal_length|petal_width|      class|
# +------------+-----------+------------+-----------+-----------+
# |         5.1|        3.5|         1.4|        0.2|Iris-setosa|
# |         4.9|        3.0|         1.4|        0.2|Iris-setosa|
# +------------+-----------+------------+-----------+-----------+
# 5- 特征工程
select_features = df.select(df["sepal_length"].cast("double"),
                            df["sepal_width"].cast("double"),
                            df["petal_length"].cast("double"),
                            df["petal_width"].cast("double"),
                            df["class"])
select_features.printSchema()
# SparkML或SparkMLlib中对应string类型可以转化为数值类型---stringIndexer
indexer = StringIndexer(inputCol="class", outputCol="indexedClass")
labelIndexer = indexer.fit(select_features)
strIndexerFeatures = labelIndexer.transform(select_features)
strIndexerFeatures.show(2)
# 使用VectorAssemble集成四个特征列为一个特征向量形式
assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
                            outputCol="features")
VecDF = assembler.transform(strIndexerFeatures)
VecDF.show(2, truncate=False)
#数据集的拆分
(trainingSet,testSet) = VecDF.randomSplit(weights=[0.8, 0.2], seed=123)
# 6- 准备机器学习的LR的算法
lr = LogisticRegression(featuresCol="features", labelCol="indexedClass", predictionCol="prediction")
# 7- 通过LR算法训练模型=数据+算法
lrModel = lr.fit(trainingSet)
# 8- 使用模型进行预测分析
y_pred = lrModel.transform(testSet)
y_pred.show(truncate=False)
#rawPrediction  原始预测值---线性回归的原始预测值
#probability 预测为各个类别的概率
#prediction  最后预测为哪一个类别的类别值
# 9- 进一步进行校验
evaluator=MulticlassClassificationEvaluator(
    predictionCol="prediction",
    labelCol="indexedClass",
    metricName="accuracy")
accuracy = evaluator.evaluate(y_pred)
print("model in test set accuracy is:%.2f%%"%(accuracy*100))
#model in test set accuracy is:97.14%
# 10- 保存模型
lrModel.write().overwrite().save("./data/lrModel/")
