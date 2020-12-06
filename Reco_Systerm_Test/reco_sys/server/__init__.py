# -*- coding:utf-8 -*-
# Desc: 初始化pool或redis构造函数
import happybase

pool = happybase.ConnectionPool(size=10, host="hadoop-master", port=9090)
import redis
from setting.default import DefaultConfig

from pyspark import SparkConf
from pyspark.sql import SparkSession
# spark配置
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_GRPC_CONFIG)

SORT_SPARK = SparkSession.builder.config(conf=conf).getOrCreate()

# 召回数据---redis--实时召回
redis_client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                 port=DefaultConfig.REDIS_PORT,
                                 db=10,
                                 decode_responses=True)

# 缓存也用redis
# 缓存在 8 号当中
cache_client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                 port=DefaultConfig.REDIS_PORT,
                                 db=8,
                                 decode_responses=True)
