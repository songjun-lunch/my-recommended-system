# -*- coding:utf-8 -*-
# Desc: recall set loader 多路召回读取
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from server import redis_client
from server import pool
import logging
from datetime import datetime
from server.utils import HBaseUtils
logger = logging.getLogger('recommend')


class ReadRecall(object):
    def __init__(self):
        self.client = redis_client
        self.hbu = HBaseUtils(pool)
        self.hot_num = 10

    def read_hbase_recall(self, table_name, key_format, column_format):
        '''读取召回结果
        '''
        reco_set = []
        try:
            data = self.hbu.get_table_cells(table_name, key_format, column_format)
            # data 是多个版本的推荐结果[[],[],[],]
            for _ in data:
                reco_set = list(set(reco_set).union(eval(_)))
        except Exception as e:
            logger.warning("{} WARN read {} recall exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        table_name,
                                                                        e))
        return reco_set

    def read_redis_new_article(self, channel_id):
        '''
        读取用户的新文章
        :param channel_id:
        :return:
        '''
        logger.info("{} Info read channel {} redis new article:".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        channel_id))
        key = 'ch:{}:new'.format(channel_id)
        # 获取所有的新文章的信息
        try:
            reco_list = self.client.zrevrange(key, 0, -1)
        except Exception as e:
            logger.warning("{} Info read  redis new article:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                       e))
            reco_list = []
        return list(map(int, reco_list))

    def read_redis_hot_article(self, channel_id):
        '''
        读取热门文章的方法
        :param channel_id:
        :return:
        '''
        logger.info("{} Info read channel {} redis hot article:".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        channel_id))
        key = 'ch:{}:hot'.format(channel_id)
        try:
            res = self.client.zrevrange(key, 0, -1)
        except Exception as e:
            logger.warning("{} Info read  redis new article:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                       e))
            res = []
        # 但是由于每个频道热门文章的数量很多，这里保留文章点击次数较多的
        res = list(map(int, res))
        if len(res) > self.hot_num:
            res = res[:self.hot_num]
        return res


if __name__ == '__main__':
    rr = ReadRecall()
    # print(rr.read_hbase_recall('cb_recall', b'recall:user:2', b'online:18'))  # als等均可
    print(rr.read_redis_new_article(18))
    print(rr.read_redis_hot_article(18))
