# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class UserInfoItem(scrapy.Item):
    # Item for user's profile information
    # user_info = scrapy.Field()
    task_id = scrapy.Field()  # 任务id
    uid = scrapy.Field()
    screen_name = scrapy.Field()
    avatar_hd = scrapy.Field()
    description = scrapy.Field()
    follow_count = scrapy.Field()
    followers_count = scrapy.Field()
    post_count = scrapy.Field()
    gender = scrapy.Field()
    verified = scrapy.Field()
    verified_reason = scrapy.Field()
    crawled_time = scrapy.Field()  # 爬取时间


class TotalNumItem(scrapy.Item):
    # Item for user's post num
    uid = scrapy.Field()
    total_num = scrapy.Field()


class UserPostItem(scrapy.Item):
    # Item for user's post content
    # user_post = scrapy.Field()
    task_id = scrapy.Field()  # 任务id
    mid = scrapy.Field()  # 推文id
    uid = scrapy.Field()  # 用户id
    text = scrapy.Field()  # 推文文本
    created_at = scrapy.Field()  # 发布时间
    source = scrapy.Field()  # 发布设备
    reposts_count = scrapy.Field()  # 转发数量
    comments_count = scrapy.Field()  # 评论数量
    attitudes_count = scrapy.Field()  # 点赞数量
    pic_num = scrapy.Field()  # 图片数量
    pics = scrapy.Field()  # 图片
    crawled_time = scrapy.Field()  # 爬取时间
    video = scrapy.Field()  # 视频

# class CommentItem(scrapy.Item):
#     # Item for comments
#     comment = scrapy.Field()


# class HotSearchItem(scrapy.Item):
#     # Item for real time hot search information
#     hot_search = scrapy.Field()
#     time_stamp = scrapy.Field()


class FansListItem(scrapy.Item):
    task_id = scrapy.Field()  # 任务id
    uid = scrapy.Field()
    fan = scrapy.Field()
    s_id = scrapy.Field()
    t_id = scrapy.Field()


# class FollowsListItem(scrapy.Item):
#     uid = scrapy.Field()
#     follower = scrapy.Field()


# class KeyWordsItem(scrapy.Item):
#     mid = scrapy.Field()  # 推文id
#     uid = scrapy.Field()  # 用户id
#     text = scrapy.Field()  # 推文文本
#     created_at = scrapy.Field()  # 发布时间
#     source = scrapy.Field()  # 发布设备
#     reposts_count = scrapy.Field()  # 转发数量
#     comments_count = scrapy.Field()  # 评论数量
#     attitudes_count = scrapy.Field()  # 点赞数量
#     pic_num = scrapy.Field()  # 图片数量
#     pics = scrapy.Field()  # 图片
#     crawled_time = scrapy.Field()  # 爬取时间
