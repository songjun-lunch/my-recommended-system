# -*- coding:utf-8 -*-
# Desc: 协议解释代码---服务器端代码
# 基于用户推荐的rpc服务推荐
# 定义指定的rpc服务输入输出参数格式proto
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from concurrent import futures
from abtest import user_reco_pb2
from abtest import user_reco_pb2_grpc
from setting.default import DefaultConfig
from server.reco_center import RecoCenter
import grpc
import time
import json
import hashlib
from setting.default import DefaultConfig, RAParam


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


def feed_recommand(user_id, channel_id, article_num, time_stamp):
    # 1-基本的变量的初始化
    class TempParam(object):
        user_id = -10
        channel_id = -10
        article_num = -10
        time_stamp = -10
        algo = ""

    temp = TempParam()
    temp.user_id = user_id
    temp.channel_id = channel_id
    temp.article_num = article_num
    temp.time_stamp = time_stamp
    # 2-实现用户分流
    # 如果用户的id为空，直接返回的null的结果
    if temp.user_id == "":
        return add_track([], temp)
    # 如果id正常的，需要经过md5分流
    code = hashlib.md5(temp.user_id.encode()).hexdigest()[:1]
    if code in RAParam.BYPASS[0]['Bucket']:
        temp.algo = RAParam.BYPASS[0]['Strategy']  # algo-1
    else:
        temp.algo = RAParam.BYPASS[1]['Strategy']  # algo-2
    # 这里的[1,2,3,4,5]是虚拟的文章的id，稍后通过推荐中心将文章返回---结合召回和排序返回
    # track = add_track([1,2,3,4,5], temp)
    track = RecoCenter().feed_recommend_time_stamp_logic(temp)
    return track


class UserRecommendServicer(user_reco_pb2_grpc.UserRecommendServicer):
    """
    对用户进行技术文章推荐
    """

    def user_recommend(self, request, context):
        """
        用户feed流推荐
        :param request:
        :param context:
        :return:
        """
        # 选择C4组合
        user_id = request.user_id
        channel_id = request.channel_id
        article_num = request.article_num
        time_stamp = request.time_stamp

        # 解析参数，并进行推荐中心推荐(暂时使用假数据替代)
        # class Temp(object):
        #     user_id = -10
        #     algo = 'test'
        #     time_stamp = -10
        #
        # tp = Temp()
        # tp.user_id = user_id
        # tp.time_stamp = time_stamp
        # _track = add_track([1,2,3,4,5], tp)

        # 解析返回参数到rpc结果参数
        # 参数如下
        # [       {"article_id": 1, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}},
        #         {"article_id": 2, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}},
        #         {"article_id": 3, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}},
        #         {"article_id": 4, "param": {"click": "", "collect": "", "share": "", 'detentionTime':''}}
        #     ]
        # _track=feed_recommand(user_id,channel_id,article_num,time_stamp)
        _track = feed_recommand(user_id, channel_id, article_num, time_stamp)

        # 第二个rpc参数
        _param1 = []
        for _ in _track['recommends']:
            # param的封装
            _params = user_reco_pb2.param2(click=_['param']['click'],
                                           collect=_['param']['collect'],
                                           share=_['param']['share'],
                                           read=_['param']['read'])
            _p2 = user_reco_pb2.param1(article_id=_['article_id'], params=_params)
            _param1.append(_p2)
        # param
        return user_reco_pb2.Track(exposure=_track['param'], recommends=_param1, time_stamp=_track['timestamp'])


#    def article_recommend(self, request, context):
#        """
#       文章相似推荐
#       :param request:
#       :param context:
#       :return:
#       """
#       # 获取web参数
#       article_id = request.article_id
#       article_num = request.article_num
#
#        # 进行文章相似推荐,调用推荐中心的文章相似
#       _article_list = article_reco_list(article_id, article_num, 105)
#
#       # rpc参数封装
#       return user_reco_pb2.Similar(article_id=_article_list)


def serve():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册本地服务
    user_reco_pb2_grpc.add_UserRecommendServicer_to_server(UserRecommendServicer(), server)
    # 监听端口
    server.add_insecure_port(DefaultConfig.RPC_SERVER)

    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


def serve():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册本地服务
    user_reco_pb2_grpc.add_UserRecommendServicer_to_server(UserRecommendServicer(), server)
    # 监听端口
    server.add_insecure_port(DefaultConfig.RPC_SERVER)

    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    # 测试grpc服务
    serve()
