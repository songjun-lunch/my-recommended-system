# -*- coding:utf-8 -*-
# Desc: online update--article
import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from online import stream_sc, SIMILAR_DS
from online import HOT_DS, NEW_ARTICLE_DS
from datetime import datetime
from setting.default import DefaultConfig
import redis
import json
import time
import setting.logging as lg

import logging
logger = logging.getLogger("online")
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.2 pyspark-shell"
# Kafka读取点击行为日志数据，获取相似文章列表
# 过滤历史文章集合
class OnlineRecall(object):
    '''
    在线的召回计算：
    1-在线内容召回，实时写入用户点击或者操作文章的相似文章
    2-在线新文章召回¡
    3-在线热门文章召回
    '''
    def __init__(self):
        # redis的IP和端口配置
        self.client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                        port=DefaultConfig.REDIS_PORT,
                                        db=10)
        # 在线召回筛选TOP-k个结果
        self.k = 20
        # pass #稍后将离线召回放入redis
    def _update_content_recall(self):
        '''
        在线内容召回---用户的行为数据
        {"actionTime":"2019-04-10 21:04:39",
        "readTime":"","channelId":18,
        "param":{"action": "click", "userId": "2", "articleId": "116644", "algorithmCombine": "C2"}}
        '''
        def get_similar_online_recall(rdd):
            '''
            从哪里获取相似文章的数据---hbase获取---happybase
            解析rdd的内容，获取进行计算
            :param rdd:
            :return:
            '''
            import happybase
            pool=happybase.ConnectionPool(size=10,host="hadoop-master",port=9090)
            for data in rdd.collect():
                #简单执行行为数据的过滤--仅仅查询clik-collect-share
                if data['param']['action'] in ['click','collect','share']:
                    logger.info("{} INFO:get user_id:{} action:{} log".format(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%s"),
                        data['param']['userId'],
                        data['param']['action']
                    ))
                    #计算读取param中的article的相似的文章
                    with pool.connection() as conn:
                        sim_table = conn.table("article_similar")
                        #根据用户的点击的信息找出最相似的10片文章
                        _dic = sim_table.row(str(data['param']['articleId']).encode(), columns=[b"similar"])
                        _srt = sorted(_dic.items(), key=lambda x: x[1], reverse=True)
                        if _srt:
                            topKSimIds = [int(i[0].split(b":")[1]) for i in _srt[:10]]

                            # 根据历史推荐集过滤，已经给用户推荐过的文章
                            history_table = conn.table("history_recall")

                            _history_data = history_table.cells(
                                b"reco:his:%s" % data["param"]["userId"].encode(),
                                b"channel:%d" % data["channelId"]
                            )

                            history = []
                            if len(_history_data) > 1:
                                for l in _history_data:
                                    history.extend(l)

                            # 根据历史召回记录，过滤召回结果
                            recall_list = list(set(topKSimIds) - set(history))
                            # 如果有推荐结果集，那么将数据添加到cb_recall表中，同时记录到历史记录表中
                            logger.info(
                                "{} INFO: store online recall data:{}".format(
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(recall_list)))
                            if recall_list:
                                recall_table = conn.table("cb_recall")
                                #将结果存储online的结果的表中---cb_recall
                                recall_table.put(
                                    b"recall:user:%s" % data["param"]["userId"].encode(),
                                    {b"online:%d" % data["channelId"]: str(recall_list).encode()}
                                )
                                #将历史的数据的信息存储在history_table中
                                history_table.put(
                                    b"reco:his:%s" % data["param"]["userId"].encode(),
                                    {b"channel:%d" % data["channelId"]: str(recall_list).encode()}
                                )
                        conn.close()
                # x可以是多次点击行为数据，同时拿到多条数据
        SIMILAR_DS.map(lambda x: json.loads(x[1])).foreachRDD(get_similar_online_recall)
    def _update_hot_redis(self):
        '''
        更新热门文章
         {"actionTime":"2019-04-10 21:04:39",
        "readTime":"","channelId":18,
        "param":{"action": "click", "userId": "2", "articleId": "116644", "algorithmCombine": "C2"}}
        :return:
        '''
        client=self.client
        def updateHotArt(rdd):
            for row in rdd.collect():
                logger.info("{}, INFO: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row))
                # 如果是曝光参数，和阅读时长选择过滤
                if row['param']['action'] == 'exposure' or row['param']['action'] == 'read':
                    pass
                else:
                    # 解析每一条的日志，分析保存点击、收藏、分享的次数，这里的行为分别自增1
                    client.zincrby("ch:{}:hot".format(row['channelId']), 1, row['param']['articleId'])
        HOT_DS.map(lambda  x:json.loads(x[1])).foreachRDD(updateHotArt)
    def _update_new_redis(self):
        client = self.client
        def computeFunction(rdd):
            for rdd in rdd.collect():
                channel_id,article_id = rdd.split(",")
                logging.info("{},INFO:get kafka new_article:channel_id:{},article_id:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),channel_id,article_id))
                client.zadd("ch:{}:new".format(channel_id),{article_id:time.time()})
        NEW_ARTICLE_DS.map(lambda x:x[1]).foreachRDD(computeFunction)
        return None
# 存入召回结果以及历史记录结果
if __name__ == '__main__':
    # 启动日志配置
    # lg.create_logger()
    op = OnlineRecall()
    # op._update_content_recall()
    # op._update_hot_redis()
    op._update_new_redis()
    stream_sc.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        pass

