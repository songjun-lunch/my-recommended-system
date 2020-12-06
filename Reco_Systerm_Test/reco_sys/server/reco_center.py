# -*- coding:utf-8 -*-
# Desc: reco_center特征中心
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
import hashlib
from setting.default import RAParam
from server.utils import HBaseUtils
from server import pool
from server.redis_cache import get_cache_from_redis_hbase
from datetime import datetime
import logging
import json
from server.recall_service import ReadRecall
#增加的召回之前的排序的代码
from server.sort_service import lr_sort_service
sort_dict={
    'LR':lr_sort_service,
}

logger = logging.getLogger('recommend')


def add_track(res, temp):
    """
    封装埋点参数
    :param res: 推荐文章id列表
    :param cb: 合并参数
    :param rpc_param: rpc参数
    :return: 埋点参数
        文章列表参数
        单文章参数
    """
    # 添加埋点参数
    track = {}

    # 准备曝光参数
    # 全部字符串形式提供，在hive端不会解析问题
    _exposure = {"action": "exposure", "userId": temp.user_id, "articleId": json.dumps(res),
                 "algorithmCombine": temp.algo}

    track['param'] = json.dumps(_exposure)
    track['recommends'] = []

    # 准备其它点击参数
    for _id in res:
        # 构造字典
        _dic = {}
        _dic['article_id'] = _id
        _dic['param'] = {}

        # 准备click参数
        _p = {"action": "click", "userId": temp.user_id, "articleId": str(_id),
              "algorithmCombine": temp.algo}

        _dic['param']['click'] = json.dumps(_p)
        # 准备collect参数
        _p["action"] = 'collect'
        _dic['param']['collect'] = json.dumps(_p)
        # 准备share参数
        _p["action"] = 'share'
        _dic['param']['share'] = json.dumps(_p)
        # 准备detentionTime参数
        _p["action"] = 'read'
        _dic['param']['read'] = json.dumps(_p)

        track['recommends'].append(_dic)

    track['timestamp'] = temp.time_stamp
    return track


class RecoCenter(object):
    '''
    推荐中心：1-处理时间戳的逻辑 2-召回-排序-缓存
    '''

    def __init__(self):
        self.hbu = HBaseUtils(pool)
        self.recall_service = ReadRecall()

    # feed推荐流的逻辑--推荐中心
    def feed_recommend_time_stamp_logic(self, temp):
        '''
        用户刷新逻辑代码
        :param temp: ABTest传入的用户的参数
        :return:
        '''
        ## 1-获取历史用户数据库最近的一次的时间last_stamp--从Hbase中获取最近的一次的数据
        try:
            last_stamp = self.hbu.get_table_row('history_recommend',
                                                'reco:his:{}'.format(temp.user_id).encode(),
                                                'channel:{}'.format(temp.channel_id).encode(),
                                                include_timestamp=True)[1]
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

        except Exception as e:
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp, exception:{}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id, e))
            last_stamp = 0
        ## 2-如果last_stamp <用户的请求的时间戳，用户进行新文章刷新的操作
        if last_stamp < temp.time_stamp:
            # 需要走正常的召回+排序或缓存的程序---稍后整合当前的召回和缓存的代码
            temp.time_stamp = int(last_stamp)
            # [44657, 14961, 17522, 43894, 44412, 17802, 14223, 18836]
            # 需要增加二级缓存
            res = get_cache_from_redis_hbase(temp, self.hbu)
            if not res:
                logger.info("{} INFO cache is null get user_id:{} channel_id:{} recall-sort data".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    temp.user_id,
                    temp.channel_id
                ))
                # 如果定义的二级缓存中都没有需要的指定数量的文章，直接重新召回+排序得到文章的推荐在返回
                res = self.user_reco_list(temp)
            track = add_track(res, temp)
            return track
        ## 3-如果last_stamp >用户的请求的时间戳，获取历史的数据
        else:
            logger.info("{} INFO read user_id:{} channel_id:{} history recommand data".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                temp.user_id,
                temp.channel_id,
            ))
            try:
                # row的信息是从hbase的history_recomnmnd中取出来一行一行的数据，下面再去执行判断™¡
                row = self.hbu.get_table_cells('history_recommend',
                                               'reco:his:{}'.format(temp.user_id).encode(),
                                               'channel:{}'.format(temp.channel_id).encode(),
                                               timestamp=temp.time_stamp + 1,  # 表示的是获取当前的时间戳的数据，加1表示的是将当前的数据加入到Hbase查询中
                                               include_timestamp=True)
            except Exception as e:
                logger.warning("{} WARN read history recommend exception:{}".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    e
                ))
                row = []
                res = []

            if not row:
                ### 3-1如果没历史数据，直接将时间戳记为0，返回的推荐的结果为空
                temp.time_stamp = 0
                res = []
            elif len(row) == 1 and row[0][1] == temp.time_stamp:
                ### 3-2如果历史数据只有一条数据，返回这一条的历史数据以及时间戳正好为请求的时间戳
                res = row[0][0]
                temp.time_stamp = 0  # 表示的是当前时间的后面的时间没有能够推荐的信息
            ### 3-3如果历史记录较多，返回的最近的一条历史记录，然后叜返回第二条历史的记录
            elif len(row) >= 2:
                res = row[0][0]
                temp.time_stamp = int(row[1][1])
            res = list(map(int, eval(res)))
            logger.info("{} WARN  history :{},{}".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                res,
                temp.time_stamp
            ))
            _track = add_track(res, temp)
            # 这里是获取历史数据，不是新请求的召回排序的刷新
            _track['param'] = ''
        return _track

    # 召回集的代码
    def user_reco_list(self, temp):
        '''
        用户在下拉刷新数据的逻辑
        1-循环算法的组合参数，遍历不同的多路召回结果
        排序部分
        2-过滤当前该请求的推荐的历史的结果，如果推荐频道和当前频道不要出现重合推荐
        3-过滤之后，选择指定个数的文章进行推荐，写入历史结果表中，剩下的写入待推荐的列表中
        :param temp:
        :return:
        '''
        # 1 - 循环算法的组合参数，遍历不同的多路召回结果
        reco_set = []
        # (1, [100, 101, 102, 103, 104], []),
        for number in RAParam.COMBINE[temp.algo][1]:
            if number == 103:
                # 从redis获取数据
                _res = self.recall_service.read_redis_new_article(temp.channel_id)
                reco_set = list(set(reco_set).union(set(_res)))
            elif number == 104:
                # 从redis获取数据
                _res = self.recall_service.read_redis_hot_article(temp.channel_id)
                reco_set = list(set(reco_set).union(set(_res)))
            else:  # 100-103的cb_recall的个性化召回
                # 从hbase获取数据
                _res = self.recall_service.read_hbase_recall(RAParam.RECALL[number][0],
                                                             'recall:user:{}'.format(temp.user_id).encode(),
                                                             '{}:{}'.format(RAParam.RECALL[number][1],
                                                                            temp.channel_id).encode())
                reco_set = list(set(reco_set).union(set(_res)))
        # 2 - 过滤当前该请求的推荐的历史的结果，如果推荐平道和当前频道不要出现重合推荐
        # 如果传如的是18号频道，还需要处理0号频道
        history_list = []
        try:
            data = self.hbu.get_table_cells('history_recommand',
                                            'reco:his:{}'.format(temp.user_id).encode(),
                                            'channel:{}'.format(temp.channel_id).encode())
            for _ in data:
                history_list = set(history_list).union(set(eval(_)))
            logger.info("{} Info  filter read userid:{} channel_id:{}".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                temp.user_id,
                temp.channel_id))
        except Exception as e:
            # 打印日志
            logger.warning("{} WARN  filter history article:{}".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                e))
        # 如果推荐频道0号和当前频道不要出现重合推荐
        try:
            data = self.hbu.get_table_cells('history_recommand',
                                            'reco:his:{}'.format(temp.user_id).encode(),
                                            'channel:{}'.format(0).encode())
            for _ in data:
                history_list = set(history_list).union(set(eval(_)))
            logger.info("{} Info  filter read userid:{} channel_id:{}".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                temp.user_id,
                temp.channel_id))
        except Exception as e:
            # 打印日志
            logger.warning("{} WARN  filter history article:{}".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                e))

        # 集合的差集---去掉历史的浏览的信息之后的数据
        reco_set = list(set(reco_set).difference(set(history_list)))
        logger.info("{} Info  after filter article  history:{}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            reco_set))
        # 3 - 过滤之后，选择指定个数的文章进行推荐，写入历史结果表中，剩下的写入待推荐的列表中
        ## 后面需要增加排序---召回之后需要进一步精排排序--reco_set的article_id
        _sortnum = RAParam.COMBINE[temp.algo][2][0]
        reco_set = sort_dict[RAParam.SORT[_sortnum]](reco_set, temp, self.hbu)
        ##排序结束---执行reco_set的排序

        if not reco_set:  # 3-1如果没有内容直接返回
            return reco_set
        else:
            # 类型的转换
            reco_set = list(map(int, reco_set))
            # 3-2 进一步判断 跟后端需要的推荐的文章的数据和article的数目
            if len(reco_set) < temp.article_num:
                # 逻辑：如果reco_Set的长度小于需要的文章的数据没直接返回--需要填充
                res = reco_set
            else:
                # 反之，需要将多余的文章加入到wait_recommand中
                # 取出推荐的内容
                res = reco_set[:temp.article_num]
                # 剩余的结果作为结果放入wait-recommend中
                self.hbu.get_table_put('wait_recommend',
                                       'reco:{}'.format(temp.user_id).encode(),
                                       'channel:{}'.format(temp.channel_id).encode(),
                                       str(reco_set[temp.article_num:]).encode(),
                                       timestamp=temp.time_stamp)
                # 历史记录日志
                self.hbu.get_table_put('history_recommend',
                                       'reco:his:{}'.format(temp.user_id).encode(),
                                       'channel:{}'.format(temp.channel_id).encode(),
                                       str(res).encode(),
                                       timestamp=temp.time_stamp)
                logger.info("{} Info  store recall userid:{} channelid:{} history data".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    temp.user_id,
                    temp.channel_id))

                return res
