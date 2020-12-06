# -*- coding:utf-8 -*-
# Desc: This is Code Desc
import redis
from server import redis_client
'''
文件不能起名redis,否则找不到redis模块
'''
#拿到redis连接
conn = redis.Redis(host='192.168.10.137', port=6379,db=10)
# # 存--"ch:18:hot"
# conn.set('ch:18:hot','11777')
# conn.set('ch:18:hot','12777')
# conn.set('ch:18:new','12777')
# conn.set('ch:18:new','13777')
# # 取
name = conn.get('ch:18:ne')  # bytes类型
print(name)

#　--------------字符串的操作----------------------------
# set(name,value,ex=None,px=None,nx=False,xx=False)
# nx: 只有name不存在时，set才执行，否则不执行 (设置为true)
# xx: 设为true, 只有name存在时，set才能执行
# conn.set('alex',14,8)  # 8秒后自动删除