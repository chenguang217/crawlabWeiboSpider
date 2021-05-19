# -*- coding: utf-8 -*-
# @Author  : CharesFuns
# @Time    : 2020/5/8 20:51
# @Function: To crawl weibo information based on key words

import json

import requests
import scrapy
import logging
import redis
import re
import os
from lxml import etree
from time import time
from math import floor
from urllib.parse import quote
from .WeiboSpider import WeiboSpider
from ..items import UserPostItem
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from urllib.request import urlretrieve
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class KeyWordsSpider(RedisSpider):
    # init parameters
    name = 'KeyWordsSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']  # crawling sites
    handle_httpstatus_list = [418]  # http status code for not ignoring
    redis_key = 'KeyWordsSpider:start_urls'

    def __init__(self, keyword, node='master', uu_id='test', page=50, *args, **kwargs):
        super(KeyWordsSpider, self).__init__(*args, **kwargs)
        self.__task_id = uu_id
        self.api = {
            'api_0': 'https://m.weibo.cn/api/container/getIndex?containerid=100103type',
            'api_1': '=61&q=',  # append keywords behind and url code this sentences
            'api_2': '&t=10&isnewpage=1&extparam=c_type=30&pos=2&mi_cid=100103&source=ranklist&flag=0&filter_type=realtimehot&cate=0',
            'api_3': '&display_time=',
            'api_4': '&luicode=10000011&lfid=231583&page_type=searchall&page=',
            'precise_time_api': 'https://m.weibo.cn/status/'
        }
        self.__weibo_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=',
                                 'api_1': '&containerid=107603', 'api_2': '&page=',
                                 'longtext_api': 'https://m.weibo.cn/statuses/extend?id=',
                                 'precise_time_api': 'https://m.weibo.cn/status/'}
        self.keyword = keyword
        self.redis_key = self.redis_key+uu_id

        if node == 'master':
            settings = get_project_settings()
            r = redis.Redis(host=settings.get("REDIS_HOST"), port=settings.get("REDIS_PORT"), decode_responses=True)
            time_stamp = floor(time())
            keyword_part = quote(self.api['api_1'] + keyword, encoding='utf-8')
            url_template = self.api['api_0'] + keyword_part + self.api['api_2'] + \
                           self.api['api_3'] + str(time_stamp) + self.api['api_4']
            u = url_template + str('1')
            print(u)
            page_num = self.parse_page_num(u)
            self.page_num = min(page_num, int(page))
            print(self.page_num)
            # self.page_num = 5
            for i in range(1, self.page_num + 1):
                url = url_template + str(i)
                request_data = {
                    'url': url,
                    'meta': {'key_words': keyword}
                }
                r.lpush(self.redis_key, json.dumps(request_data))
                # yield scrapy.Request(url=url, callback=self.parse, meta={'key_words': keywords})

    # Override父类的make_request_from_data方法，解析json生成Request
    def make_request_from_data(self, data):
        data = json.loads(data)
        url = data.get('url')
        meta = data.get('meta')
        print("Fetch url:", url)
        return scrapy.Request(url=url, callback=self.parse, meta=meta, dont_filter=True)

    def parse_page_num(self, url):
        try:
            content = requests.get(url).text
        except IOError:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")  # , level=logging.ERROR)
            return 3
        if 'data' not in content:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")# , level=logging.ERROR)
            return 3
        content_dict = json.loads(content)
        post_count = content_dict['data']['cardlistInfo']['total']
        page_num = min(1000, post_count)//20 + 1
        return page_num

    def parse(self, response):
        data = json.loads(response.text)['data']
        cards = data['cards']
        for card in cards:
            if card['card_type'] == 9:
                mblog = card['mblog']

                # 爬取图片
                for i in range(min(9, mblog['pic_num'])):
                    pic = mblog['pics'][i]
                    pic_url = 'https://wx3.sinaimg.cn/large/'+pic['pid']+'.jpg'
                    # pic_url = pic['url']
                    # urlretrieve(pic_url, './img/keywords/' + self.__task_id + '_' + mblog['mid'] + '_' + str(i) + '.jpg')
                    # mblog['pics'][i] = './img/posts/' + self.__task_id + '_' + mblog['mid'] + '_' + str(i) + '.jpg'
                    if not os.path.exists('/data/' + self.__task_id + '/img/'):
                        os.makedirs('/data/' + self.__task_id + '/img/')
                    urlretrieve(pic_url, '/data/' + self.__task_id + '/img/' + mblog['mid'] + '_' + str(i) + '.jpg')
                    mblog['pics'][i] = '/data/' + self.__task_id + '/img/' + mblog['mid'] + '_' + str(i) + '.jpg'

                #  下载视频
                if 'page_info' in mblog and mblog['page_info']['type'] == 'video':
                    vidoe_url = mblog['page_info']['media_info']['stream_url_hd']
                    res = requests.get(vidoe_url, stream=True)
                    if not os.path.exists('/data/' + self.__task_id + '/video/'):
                        os.makedirs('/data/' + self.__task_id + '/video/')
                    # with open('./video/' + self.__task_id + '_' + mblog['mid'] + '.mp4', "wb") as mp4:
                    with open('/data/' + self.__task_id + '/video/' + mblog['mid']+'.mp4', "wb") as mp4:
                        for chunk in res.iter_content(
                                chunk_size=1024 * 1024):
                            if chunk:
                                mp4.write(chunk)
                    mblog['video'] = '/data/' + self.__task_id + '/video/' + mblog['mid']+'.mp4'
                else:
                    mblog['video'] = None

                if mblog['isLongText']:
                    longtext_url = self.__weibo_info_api['longtext_api'] + mblog['id']
                    yield scrapy.Request(url=longtext_url, callback=self.parse_longtext,
                                         meta={'post_item': mblog})
                else:
                    item = self.parse_field(mblog)
                    yield item

    def parse_longtext(self, response):
        # parser for longtext post
        user_post_item = response.meta['post_item']
        data = json.loads(response.text)['data']
        user_post_item['text'] = data['longTextContent']
        item = self.parse_field(user_post_item)
        yield item

    def parse_field(self, item):
        user_post_item = UserPostItem()
        user_post_item['dataType'] = 1
        user_post_item['mid'] = item['mid']
        user_post_item['uu_id'] = self.__task_id
        user_post_item['uid'] = item['user']['id']
        user_post_item['text'] = item['text']
        user_post_item['created_at'] = item['created_at']
        user_post_item['source'] = item['source']
        user_post_item['reposts_count'] = item['reposts_count']
        user_post_item['comments_count'] = item['comments_count']
        user_post_item['attitudes_count'] = item['attitudes_count']
        user_post_item['pic_num'] = item['pic_num']
        user_post_item['video'] = item['video']
        if item['pic_num'] > 0:
            user_post_item['pics'] = item['pics']
        else:
            user_post_item['pics'] = None
        return user_post_item

