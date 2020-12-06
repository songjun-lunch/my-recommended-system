# -*- coding:utf-8 -*-
# Desc: 通过Spark的ALS算法完成推荐实践--给出用户的推荐的结果
# 0userId::2itemId::3rating评分
import os

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 如果有多个版本的python需要指定，如果不指定可能会报错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkConf

# 分析代码实现的步骤
# 1-准备Spark的环境
conf = SparkConf().setMaster("local[*]").setAppName("SparkALSTest")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
lines = spark.read.text("./data/sample_movielens_data.txt").rdd
# print(lines.collect())#Row(value='0userid::2itemid::3rating')
parts = lines.map(lambda x: x.value.split("::"))
ratingRDD = parts.map(lambda x: Row(userId=int(x[0]), itemId=int(x[1]), rating=float(x[2])))
# 增加schema信息
ratings = spark.createDataFrame(ratingRDD)
# 2-数据的基本的信息的查看
ratings.printSchema()
# root
#  |-- itemId: long (nullable = true)
#  |-- rating: double (nullable = true)
#  |-- userId: long (nullable = true)
# ratings.show(5)
# +------+------+------+
# |itemId|rating|userId|
# +------+------+------+
# |     2|   3.0|     0|
# |     3|   1.0|     0|
# |     5|   2.0|     0|
# |     9|   4.0|     0|
# |    11|   1.0|     0|
# +------+------+------+
# only showing top 5 rows
# 3-特征工程
# 数据集的训练集和测试集的拆分
(training, test) = ratings.randomSplit(weights=[0.8, 0.2])
# 4-准备ALS算法
als = ALS(rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
          implicitPrefs=False, alpha=1.0, userCol="userId", itemCol="itemId", seed=None,
          ratingCol="rating")
# 5-ALS算法的训练
model = als.fit(training)
# 7-ALS算法的预测
predictions = model.transform(test)
# predictions.show(5)
# 8-保存ALS算法
# model.save("path")
# 9-对ALS算法进行校验----回归问题的参考的方法
evaluator=RegressionEvaluator(predictionCol="prediction", labelCol="rating",
                 metricName="rmse")#rmse-root mean square error
rmse = evaluator.evaluate(predictions)
print("model rmase result is:",rmse)#model rmase result is: 0.8959804230596524
# 9-ALS算法的预测
usersRec = model.recommendForAllUsers(10)
itemsRec = model.recommendForAllItems(10)
print("userRec value is:")
usersRec.show()
# +------+--------------------+
# |userId|     recommendations|
# +------+--------------------+
# |    28|[[92,4.502435], [...|
# |    26|[[94,4.606663], [...|
# |    27|[[18,3.151648], [...|
# |    12|[[27,4.263776], [...|
print("itemsRec value is:")
itemsRec.show()
# +------+--------------------+
# |itemId|     recommendations|
# +------+--------------------+
# |    31|[[12,3.068076], [...|
# |    85|[[16,3.978181], [...|
# |    65|[[23,4.034745], [...|

from pyspark.ml.evaluation import MulticlassClassificationEvaluator,BinaryClassificationEvaluator