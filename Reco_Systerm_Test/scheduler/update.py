# -*- coding:utf-8 -*-
# Desc: update file
from offline.update_article import UpdateArticle
from offline.update_user import UpdateUserProfile
from offline.update_recall import UpdateRecall
from offline.update_feature import FeaturePlatform
def update_article_profile():
    '''
    定时更新文章画像
    '''
    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    # 根据得到的合并的数据处理得到tfidf的值和textrank的值
    if sentence_df.rdd.collect():
        textrank_keywords_df, keywordsIndex = ua.generate_article_label()
        artcleProfile = ua.get_article_profile(textrank_keywords_df, keywordsIndex)
        ua.compute_article_similar(artcleProfile)

def update_user_profile():
    '''
    定时更新用户画像
    :return:
    '''
    up = UpdateUserProfile()
    if up.update_user_action_basic():
        up.update_user_label()
        up.update_user_info()

def update_user_recall():
    '''
    定时更新用户的召回结果
    :return:
    '''
    ur=UpdateRecall(500)
    ur.update_als_recall()
    ur.update_content_recall()

def update_ctr_features():
    '''
    定时更新文章的和用户的特征--特征中心
    :return:
    '''
    fp = FeaturePlatform()
    fp.update_user_ctr_feature_to_hbase()
    fp.update_article_ctr_feature_to_hbase()
