import os
import re
import json
import time

import scrapy
import logging
import redis
import requests
from lxml import etree
from ..mongo_util import mongo_util
from ..items import *
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from urllib.parse import quote
from distutils.util import strtobool
from urllib.request import urlretrieve
from math import floor
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class ImportantUserSpider(RedisSpider):
    # init parameters
    name = 'ImportantUserSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']  # crawling sites
    handle_httpstatus_list = [418]  # http status code for not ignoring
    redis_key = 'ImportantUserSpider:start_urls'

    def __init__(self, schedule='False', *args, **kwargs):
        super(ImportantUserSpider, self).__init__(*args, **kwargs)
        self.schedule = strtobool(schedule)
        self.start_urls = ['https://m.weibo.cn/']
        self.time_arg = floor(time.time())  # get time seconds as the essential arguments for crawling hot searches
        self.api = {
            'api_0': 'https://m.weibo.cn/api/container/getIndex?containerid=100103type',
            'api_1': '=61&q=',  # append keywords behind and url code this sentences
            'api_2': '&t=10&isnewpage=1&extparam=c_type=30&pos=2&mi_cid=100103&source=ranklist&flag=0&filter_type=realtimehot&cate=0',
            'api_3': '&display_time=',
            'api_4': '&luicode=10000011&lfid=231583&page_type=searchall&page=',
            'precise_time_api': 'https://m.weibo.cn/status/'
        }
        self.target_url = f'https://m.weibo.cn/api/container/getIndex?containerid=106003type%3D25%26t%3D3%26' \
                            f'disable_hot%3D1%26filter_type%3Drealtimehot&title=%E5%BE%AE%E5%8D%9A%E7%83%AD%E6%90%9C' \
                            f'&extparam=cate%3D10103%26pos%3D0_0%26mi_cid%3D100103%26filter_type%3Drealtimehot%' \
                            f'26c_type%3D30%26 display_time%3D{self.time_arg}&luicode=10000011&lfid=231583'

        settings = get_project_settings()
        self.r = redis.Redis(host=settings.get("REDIS_HOST"), port=settings.get("REDIS_PORT"), password=settings.get("REDIS_PARAMS")['password'], decode_responses=True)
        request_data = {
            'url': self.target_url,
            'meta': {'repeat_times': 0},
            'callback': "parse_hot_search"
        }
        self.r.lpush(self.redis_key, json.dumps(request_data))
        # scrapy.Request(url=self.target_url, callback=self.parse_hot_search, meta={'repeat_times': 0})

    def make_request_from_data(self, data):
        data = json.loads(data)
        url = data.get('url')
        callback = data.get('callback')
        meta = data.get('meta')
        print("Fetch url:", url)
        logging.log(msg="Fetch url:" + url, level=logging.INFO)
        if callback == "parse_hot_search":
            return scrapy.Request(url=url, callback=self.parse_hot_search, meta=meta, dont_filter=True)
        elif callback == "parse_user":
            return scrapy.Request(url=url, callback=self.parse_user, dont_filter=True)

    def parse_hot_search(self, response):
        cards = json.loads(response.text)['data']['cards']
        for i in range(2):
            for hot_search in cards[i]["card_group"]:
                kw = "#" + hot_search["desc"] + "#"
                print(kw)
                keyword_part = quote(self.api['api_1'] + kw, encoding='utf-8')
                url_template = self.api['api_0'] + keyword_part + self.api['api_2'] + \
                               self.api['api_3'] + str(self.time_arg) + self.api['api_4']
                u = url_template + str('1')

                # for i in range(5):
                #     page_num = self.parse_page_num(u)
                #     if page_num > 1:
                #         break
                # self.page_num = page_num
                self.page_num = 5
                for i in range(1, self.page_num + 1):
                    url = url_template + str(i)
                    yield scrapy.Request(url=url, callback=self.parse_user, dont_filter=True, meta={'key_words': kw})

    def parse_user(self, response):
        data = json.loads(response.text)['data']
        cards = data['cards']
        user_info_item = UserInfoItem()
        for card in cards:
            if card['card_type'] == 9:
                user_info = card['mblog']['user']
                if user_info['followers_count'] > 100000:
                    print(user_info)
                    user_info_item['dataType'] = 0
                    user_info_item['uu_id'] = 12345
                    user_info_item['uid'] = user_info['id']
                    user_info_item['screen_name'] = user_info['screen_name']
                    user_info_item['avatar_hd'] = user_info['avatar_hd']
                    user_info_item['description'] = user_info['description']
                    user_info_item['follow_count'] = user_info['follow_count']
                    user_info_item['followers_count'] = user_info['followers_count']
                    user_info_item['post_count'] = user_info['statuses_count']
                    user_info_item['gender'] = user_info['gender']
                    user_info_item['verified'] = user_info['verified']
                    try:
                        user_info_item['verified_reason'] = user_info['verified_reason']
                    except:
                        user_info_item['verified_reason'] = ''
                    print(user_info_item['screen_name'])
                    yield user_info_item


    def parse_page_num(self, url):
        try:
            content = requests.get(url).text
        except IOError:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error! url:" + url)  # , level=logging.ERROR)
            return 3
        if 'data' not in content:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error! url:" + url)# , level=logging.ERROR)
            return 3
        try:
            content_dict = json.loads(content)
            post_count = content_dict['data']['cardlistInfo']['total']
            page_num = post_count//10 + 1
        except:
            page_num = 3
            self.logger.info(msg="[weibo_info_spider] empty response! url:" + url)
        return page_num

