# -*- coding:utf-8 -*-
# Desc: 初始化环境变量的结构文件
from pyspark import SparkConf
from pyspark.sql import SparkSession

class SparkSessionBase(object):
    SPARK_APP_NAME = None  # APP的名字
    SPARK_URL = "yarn"  # 启动运行方式

    SPARK_EXECUTOR_MEMORY = "2g"  # 执行内存
    SPARK_EXECUTOR_CORES = 2  # 每个EXECUTOR能够使用的CPU core的数量
    SPARK_EXECUTOR_INSTANCES = 2  # 最多能够同时启动的EXECUTOR的实例个数

    ENABLE_HIVE_SUPPORT = False
    def _create_spark_session(self):
        conf = SparkConf()
        config=(
            ("spark.app.name",self.SPARK_APP_NAME),
            ("spark.executor.memory",self.SPARK_EXECUTOR_MEMORY),
            ("spark.master",self.SPARK_URL),
            ("spark.executor.cores",self.SPARK_EXECUTOR_CORES),
            ("spark.executor.instances",self.SPARK_EXECUTOR_INSTANCES),
        )
        conf.setAll(config)
        #读取配置文件
        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()

    def _create_spark_hbase(self):
        conf = SparkConf()
        config=(
            ("spark.app.name",self.SPARK_APP_NAME),
            ("spark.executor.memory",self.SPARK_EXECUTOR_MEMORY),
            ("spark.master",self.SPARK_URL),
            ("spark.executor.cores",self.SPARK_EXECUTOR_CORES),
            ("spark.executor.instances",self.SPARK_EXECUTOR_INSTANCES),
            ("hbase.zookeeper.quorum","192.168.19.137"),
            ("hbase.zookeeper.property.clientPort","22181")

        )
        conf.setAll(config)
        #读取配置文件
        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()