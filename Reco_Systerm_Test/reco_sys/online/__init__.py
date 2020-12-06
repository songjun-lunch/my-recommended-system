# -*- coding:utf-8 -*-
# Desc: sparkstreaming和kafka的配置
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from setting.default import DefaultConfig

# 1-创建SparkStreming context的配置
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_ONLINE_CONFIG)
sc = SparkContext(conf=conf)
stream_sc = StreamingContext(sc, 60)

# 2-和kafka的配置
similar_kafkaParams = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER, "group.id": 'similar'}
SIMILAR_DS = KafkaUtils.createDirectStream(stream_sc, ["click-trace"], similar_kafkaParams)
# 3-其他配置
# --热门文章
kafka_params = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER}
HOT_DS = KafkaUtils.createDirectStream(stream_sc, ["click-trace"], kafka_params)
# --新文章
NEW_ARTICLE_DS = KafkaUtils.createDirectStream(stream_sc, ['new_article'], kafka_params)
