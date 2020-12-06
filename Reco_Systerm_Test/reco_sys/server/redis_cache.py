# -*- coding:utf-8 -*-
# Desc: redis缓存策略估计
from server import cache_client
import logging
from datetime import datetime

logger = logging.getLogger('recommend')


def get_cache_from_redis_hbase(temp, hbu):
    '''
    读取用户推荐数据缓存结果
    :param temp: 传入的基本信息--从客户端传入
    :param hbu:hbase读取
    :return:
    '''
    # 1-从redis的8号数据读取数据
    key = 'reco:{}:{}:art'.format(temp.user_id, temp.channel_id)
    res = cache_client.zrevrange(key, 0, temp.article_num - 1)
    if res:
        # 2-如果redis中有数据的，进行wait_recommand的推荐，放入redis中
        cache_client.zrem(key, *res)
    else:
        # 定义一组list存放缓存结果
        hbase_cache = []
        try:
            # 如果redis中没有数据的，需要从hbase中获取数据
            hbase_cache = eval(hbu.get_table_row('wait_recommend',
                                                 'reco:{}'.format(temp.user_id).encode(),
                                                 'channel:{}'.format(temp.channel_id).encode()))
        except Exception as e:
            logger.warning("{} WARN read userid:{} wait_recommand:{} not exists".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                temp.user_id,
                e
            ))
            hbase_cache = []
        # 3-将取出的结果放入历史结果中
        if not hbase_cache:
            return hbase_cache
        if len(hbase_cache) > 100:
            # 日志信息的打印
            logger.info("{} Info reduce cache  userid:{} channel:{} wait_recommed data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                temp.user_id,
                temp.channel_id
            ))
            redis_cache = hbase_cache[:100]
            cache_client.zadd(key, dict(zip(redis_cache, range(len(redis_cache)))))
            # 将数据放入
            # 剩下的放回 wait hbase 结果
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'channel:{}'.format(temp.channel_id).encode(),
                              str(hbase_cache[100:]).encode(),
                              timestamp=temp.time_stamp)
        else:
            # 日志信息的打印
            logger.info("{} Info delete userid:{} channel:{} cache data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                temp.user_id,
                temp.channel_id
            ))
            cache_client.zadd(key, dict(hbase_cache, range(len(hbase_cache)))) \
                # 如果redis中没有数据的，需要从hbase中获取数据
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'channel:{}'.format(temp.channel_id).encode(),
                              str([]).encode(),
                              timestamp=temp.time_stamp)
        res = cache_client.zrevrange(key, 0, len(temp.article_num - 1))
        if res:
            cache_client.zrem(key, *res)
    # 存放进入历史的推荐表中
    res = list(map(int, res))
    # 日志信息的打印
    logger.info("{} Info get cache data and store userid:{} channel:{} cache data".format(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        temp.user_id,
        temp.channel_id
    ))
    hbu.get_table_put('history_recommend',
                      'reco:his:{}'.format(temp.user_id).encode(),
                      'channel:{}'.format(temp.channel_id).encode(),
                      str(res).encode(),
                      timestamp=temp.time_stamp)
    return res
