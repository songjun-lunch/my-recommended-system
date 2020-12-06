# -*- coding:utf-8 -*-
# Desc: 主调函数
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from scheduler.update import update_article_profile
from scheduler.update import update_user_profile
from scheduler.update import update_user_recall
from scheduler.update import update_ctr_features

import setting.logging as lg
lg.create_logger()

# 创建scheduler，多进程执行
executors = {
    'default': ProcessPoolExecutor(3)
}

scheduler = BlockingScheduler(executors=executors)
# 定时文章画像
scheduler.add_job(update_article_profile, trigger="interval", hours=1)
# 定时更新用户画像
scheduler.add_job(update_user_profile,trigger="interval",hours=2)
# 定时更新用户召回的信息
scheduler.add_job(update_user_recall,trigger="interval",hours=3)
# 定时更新特征中心的信息
scheduler.add_job(update_ctr_features,trigger="interval",hours=4)
#启动python调用的执行
scheduler.start()