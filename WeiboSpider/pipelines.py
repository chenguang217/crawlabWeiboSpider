# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import time
from .items import *
from .database_tool import DBConnector
from scrapy.exceptions import DropItem
from pymongo.errors import DuplicateKeyError


class WeibospiderPipeline(object):
    # count =0

    def __init__(self):
        # to check the __uid from TotalNumItem, means just need to save one item and drop others
        db_connector = DBConnector()
        self.__hash_uid_list = []
        self.db, self.client = db_connector.create_mongo_connection()

    def get_crawled_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        try:
            crawled_time = self.get_crawled_time()
            if isinstance(item, UserPostItem):
                # self.count = self.count + 1
                post_id = item['mid']
                item['crawled_time'] = crawled_time
                self.db['post'].update({"mid": post_id}, {'$set': item},  upsert=True)
                # db.getCollection('post').update({'id':'3620163264292880'},{'$set':{'comments':[]}},upsert=true)
                return item

            # db.getCollection('post').update({'id':'3623774613725468'},{'$push':{'comments':{'pid':'1223','content':'lena'}}},upsert=true)

            elif isinstance(item, TotalNumItem):
                uid_hash = hash(item['uid'])
                insert_dict = {'uid': item['uid'], 'total_num': item['total_num'],
                               'crawled_time': crawled_time}  # item to dict?
                if uid_hash in self.__hash_uid_list:
                    raise DropItem("Repeating TotalNumItem.")
                else:
                    self.__hash_uid_list.append(uid_hash)
                    uid = insert_dict['uid']
                    self.db['total_num'].update({'uid': uid}, {'$set': insert_dict},  upsert=True)
                    return item

            elif isinstance(item, UserInfoItem):
                # uid = item['user_info']['id']
                uid = item['uid']
                item['crawled_time'] = crawled_time
                self.db['user'].update({'uid': uid}, {'$set': item},  upsert=True)
                return item

            # elif isinstance(item, HotSearchItem):
            #     insert_dic = {'content': item['hot_search'], 'time_stamp': item['time_stamp']}
            #     self.db['hot_search'].update({'time_stamp': item['time_stamp']},
            #                                  {'$set': insert_dic}, upsert=True)
            #     return item

            elif isinstance(item, FansListItem):
                # need to filter duplicating users
                item_dict = dict(item)
                item_dict['crawled_time'] = self.get_crawled_time()
                self.db['followers'].insert_one(item_dict)
                return item

            # elif isinstance(item, FollowsListItem):
            #     item_dict = dict(item)
            #     item_dict['crawled_time'] = self.get_crawled_time()
            #     self.db['follows'].insert_one(item_dict)
            #     return item

            # elif isinstance(item, KeyWordsItem):
            #     item_dict = dict(item)
            #     item_dict['crawled_time'] = self.get_crawled_time()
            #     self.db['key_words'].update_one({'post.id': item_dict['post']['id']}, {'$set': item_dict}, upsert=True)
        except DuplicateKeyError:
            raise DropItem


