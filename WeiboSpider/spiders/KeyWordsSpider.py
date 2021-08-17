import json

import requests
import scrapy
import logging
from ..database_tool import DBConnector
import redis
import re
import os
import time
from math import floor
from urllib.parse import quote
from ..items import UserPostItem
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from distutils.util import strtobool
from urllib.request import urlretrieve
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class KeyWordsSpider(RedisSpider):
    # init parameters
    name = 'KeyWordsSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']  # crawling sites
    handle_httpstatus_list = [418]  # http status code for not ignoring
    redis_key = 'KeyWordsSpider:start_urls'

    def __init__(self, keyword, node='master', uu_id='test', page=200, operation="or", crawl_image='False', crawl_video='False', important_user='False', schedule='False', *args, **kwargs):
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
        # self.keyword = keyword
        self.keywords = list(filter(None, keyword.split('|')))
        self.operation = operation
        self.redis_key = self.redis_key+uu_id
        self.crawl_image = strtobool(crawl_image)
        self.crawl_video = strtobool(crawl_video)
        self.important_user = strtobool(important_user)
        self.node = node
        self.schedule = strtobool(schedule)
        if self.important_user:
            self.uid_list = self.get_important_user()

        if not os.path.exists('/data/'):
            os.makedirs('/data/')

        if node == 'master':
            settings = get_project_settings()
            r = redis.Redis(host=settings.get("REDIS_HOST"), port=settings.get("REDIS_PORT"), decode_responses=True)
            time_stamp = floor(time.time())

            for kw in self.keywords:
                keyword_part = quote(self.api['api_1'] + kw, encoding='utf-8')
                url_template = self.api['api_0'] + keyword_part + self.api['api_2'] + \
                               self.api['api_3'] + str(time_stamp) + self.api['api_4']
                u = url_template + str('1')
                # print(u)
                # page_num = self.parse_page_num(u)
                for i in range(5):
                    page_num = self.parse_page_num(u)
                    if page_num > 1:
                        break
                self.page_num = min(page_num, int(page))
                print(self.page_num)
                # self.page_num = 5
                for i in range(1, self.page_num + 1):
                    url = url_template + str(i)
                    request_data = {
                        'url': url,
                        'meta': {'key_words': kw}
                    }
                    r.lpush(self.redis_key, json.dumps(request_data))

    # Override父类的make_request_from_data方法，解析json生成Request
    def make_request_from_data(self, data):
        data = json.loads(data)
        url = data.get('url')
        meta = data.get('meta')
        print("Fetch url:", url)
        logging.log(msg="Fetch url:"+url, level=logging.INFO)
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
        page_num = post_count//10 + 1
        return page_num

    def parse(self, response):
        data = json.loads(response.text)['data']
        cards = data['cards']
        for card in cards:
            if card['card_type'] == 9:
                mblog = card['mblog']

                if self.important_user and mblog['user']['id'] not in self.uid_list:
                    return

                detail_url = "https://m.weibo.cn/detail/" + mblog['mid']
                yield scrapy.Request(url=detail_url, callback=self.parse_text,
                                     meta={'post_item': mblog})

    def parse_text(self, response):
        user_post_item = response.meta['post_item']

        # 爬取图片
        if self.crawl_image:
            for i in range(min(9, user_post_item['pic_num'])):
                pic = user_post_item['pics'][i]
                pic_url = 'https://wx3.sinaimg.cn/large/' + pic['pid'] + '.jpg'
                file_name = self.__task_id + '_' + user_post_item['mid'] + '_' + str(i) + '.jpg'
                # self.mongo.save_image(pic_url, file_name)
                # mblog['pics'][i] = file_name
                urlretrieve(pic_url, '/data/' + file_name)
                try:
                    self.fileUpload('/data/' + file_name, file_name)
                except:
                    logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [KeyWordsSpider] ")
                                    + "KeyWordsSpider" + ": failed to upload image:"
                                    + file_name, level=logging.INFO)
                user_post_item['pics'][i] = '/data/' + file_name
        else:
            user_post_item['pics'] = None

        if self.crawl_video:
            #  下载视频
            if 'page_info' in user_post_item and user_post_item['page_info']['type'] == 'video':
                video_url = user_post_item['page_info']['media_info']['stream_url_hd']
                file_name = self.__task_id + '_' + user_post_item['mid'] + '.mp4'
                # self.mongo.save_video(video_url, file_name)
                # mblog['video'] = file_name
                try:
                    res = requests.get(video_url, stream=True)
                    with open('/data/' + file_name, "wb") as mp4:
                        for chunk in res.iter_content(
                                chunk_size=1024 * 1024):
                            if chunk:
                                mp4.write(chunk)
                    if os.path.getsize('/data/' + file_name) > 500 * 1024:
                        self.fileUpload('/data/' + file_name, file_name)
                        user_post_item['video'] = '/data/' + file_name
                    else:
                        user_post_item['video'] = None
                except:
                    logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [KeyWordsSpider] ")
                                    + "KeyWordsSpider" + ": failed to upload video:"
                                    + file_name, level=logging.INFO)
                    user_post_item['video'] = None
            else:
                user_post_item['video'] = None
        else:
            user_post_item['video'] = None

        if user_post_item['isLongText']:
            longtext_url = self.__weibo_info_api['longtext_api'] + user_post_item['id']
            yield scrapy.Request(url=longtext_url, callback=self.parse_longtext,
                                 meta={'post_item': user_post_item})
        else:
            item = self.parse_field(user_post_item)
            if self.operation == "and":
                for kw in self.keywords:
                    if kw not in item['text']:
                        return
            yield item

    def parse_longtext(self, response):
        # parser for longtext post
        user_post_item = response.meta['post_item']
        try:
            data = json.loads(response.text)['data']
            user_post_item['text'] = data['longTextContent']
            item = self.parse_field(user_post_item)
        except:
            return
        if self.operation == "and":
            for kw in self.keywords:
                if kw not in item['text']:
                    return
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

    def get_important_user(self):
        db_connector = DBConnector()
        db, client = db_connector.create_mongo_connection()
        uid_list = db['uidList'].find()
        uid_list = list(uid_list[0].keys())[1:]
        return uid_list

    def fileUpload(self, file_path, file_name):
        upload_url = 'http://192.168.0.230:8888/upload'
        header = {"ct": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}
        files = {'file': open(file_path, 'rb')}
        # upload_data = {"parentId": "", "fileCategory": "personal", "fileSize": 179, "fileName": file_name, "uoType": 1}
        upload_data = {"fileName": file_name}
        upload_res = requests.post(upload_url, upload_data, files=files, headers=header)
        return upload_res.text