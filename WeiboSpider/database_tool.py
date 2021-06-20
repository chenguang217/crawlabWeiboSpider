# -*- coding: utf-8 -*-
# @Author  : CharesFuns
# @Time    : 2020/6/21 0:38
# @Function: 

import pymongo
from scrapy.utils.project import get_project_settings


class DBConnector:
    def __init__(self):
        # 重写该类或者填充本地数据库配置信息
        settings = get_project_settings()
        self.mongo_host = settings.get("MONGO_HOST")
        self.mongo_port = settings.get("MONGO_PORT")
        # self.mongo_uri = "mongodb://localhost:27017/"
        self.mongo_database = "weibo"
        # self.mongo_user_name = 'admin'
        # self.mongo_pass_wd = "123456"

    def create_mongo_connection(self):
        # client = pymongo.MongoClient(self.mongo_host, self.mongo_port)
        client = pymongo.MongoClient(self.mongo_host, self.mongo_port)
        database = client[self.mongo_database]
        # database.authenticate(self.mongo_user_name, self.mongo_pass_wd)
        return database, client
