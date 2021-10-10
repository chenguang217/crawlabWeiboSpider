import os
import json
import time

import logging
import redis
import requests
from ..items import *
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from distutils.util import strtobool
from urllib.request import urlretrieve
from ..database_tool import DBConnector
from multiprocessing import Process
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


# @staticmethod
def init_process(host, port, password, uid_list, redis_key):
    print("进入子进程")
    user_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=', 'api_1': '&containerid=100505'}
    weibo_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=',
                      'api_1': '&containerid=107603', 'api_2': '&page=',
                      'longtext_api': 'https://m.weibo.cn/statuses/extend?id=',
                      'precise_time_api': 'https://m.weibo.cn/status/'}
    r = redis.Redis(host=host, port=port,
                    password=password, decode_responses=True)

    for u in uid_list:
        # 向Redis存入初始请求
        u = str(u)
        user_info_url = 'https://m.weibo.cn/' + user_info_api['api_0'] + \
                        u + user_info_api['api_1'] + u
        # 获取总页数
        page_num = 0
        for i in range(5):
            try:
                content = requests.get(user_info_url).text
                content_dict = json.loads(content)
                statuses_count = content_dict.get('data').get('userInfo')["statuses_count"]
                page_num = statuses_count // 10 + 1
            except IOError:
                page_num = 0
            except TypeError:
                page_num = 0
            if page_num > 0:
                break
            else:
                page_num = 1
        weibo_page_range = min(page_num, 199)

        weibo_info_urls = []
        for i in range(0, weibo_page_range + 1):
            weibo_info_url = 'https://m.weibo.cn/' + weibo_info_api['api_0'] + u + \
                             weibo_info_api['api_1'] + u + weibo_info_api['api_2'] + str(i)
            # print(weibo_info_url)
            weibo_info_urls.append(weibo_info_url)

        for weibo_info_url in weibo_info_urls:
            request_data = {
                'url': weibo_info_url,
                'callback': "parse_post",
                'meta': None
            }
            r.lpush(redis_key, json.dumps(request_data))


class BigScaleWeiboSpider(RedisSpider):
    # init parameters
    name = 'BigScaleWeiboSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']  # crawling sites
    handle_httpstatus_list = [418]  # http status code for not ignoring
    redis_key = 'BigScaleWeiboSpider:start_urls'

    def __init__(self, node='master', uu_id='1996', page=199, crawl_image='False',
                 crawl_video='False', schedule='False', *args, **kwargs):
        super(BigScaleWeiboSpider, self).__init__(*args, **kwargs)
        self.start_urls = ['https://m.weibo.cn/']
        self.__uid_list = self.get_important_user()
        # self.__uid = uid
        self.__task_id = uu_id
        self.__user_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=', 'api_1': '&containerid=100505'}
        self.__weibo_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=',
                                 'api_1': '&containerid=107603', 'api_2': '&page=',
                                 'longtext_api': 'https://m.weibo.cn/statuses/extend?id=',
                                 'precise_time_api': 'https://m.weibo.cn/status/'}
        self.__weibo_page_range = int(page)
        self.redis_key = self.redis_key + uu_id
        self.crawl_image = strtobool(crawl_image)
        self.crawl_video = strtobool(crawl_video)
        self.schedule = strtobool(schedule)
        # if(self.crawl_image or self.crawl_video):
        #     self.mongo = mongo_util()

        if not os.path.exists('/data/'):
            os.makedirs('/data/')

        if node == 'master':
            # 获取redis连接
            settings = get_project_settings()
            dict = {}
            dict['host'] = settings.get("REDIS_HOST")
            dict['port'] = settings.get("REDIS_PORT")
            dict['password'] = settings.get("REDIS_PARAMS")['password']
            dict['uid_list'] = self.__uid_list
            dict['redis_key'] = self.redis_key
            p = Process(target=init_process, kwargs=dict)
            p.start()

            # r = redis.Redis(host=settings.get("REDIS_HOST"), port=settings.get("REDIS_PORT"),
            #                 password=settings.get("REDIS_PARAMS")['password'], decode_responses=True)
            #
            # for u in self.__uid_list:
            #     # 向Redis存入初始请求
            #     u = str(u)
            #     user_info_url = self.crawling_user_info(u)  # 拼接用户信息URL
            #     # 获取总页数
            #     # page_num = self.parse_page_num(user_info_url)
            #     for i in range(5):
            #         page_num = self.parse_page_num(user_info_url)
            #         if page_num > 0:
            #             break
            #         else:
            #             page_num = 1
            #     self.__weibo_page_range = min(page_num, int(page))
            #     # r.lpush(self.redis_key, user_info_url)
            #     request_data = {
            #         'url': user_info_url,
            #         'callback': "parse_user",
            #         'meta': {'repeat_times': 0}
            #     }
            #     r.lpush(self.redis_key, json.dumps(request_data))
            #
            #     weibo_info_urls = self.crawling_post_info(u)  # 拼接博文URL
            #     for weibo_info_url in weibo_info_urls:
            #         # r.lpush(self.redis_key, user_info_url)
            #         request_data = {
            #             'url': weibo_info_url,
            #             'callback': "parse_post",
            #             'meta': None
            #         }
            #         r.lpush(self.redis_key, json.dumps(request_data))

    def parse_page_num(self, url):
        try:
            content = requests.get(url).text
            content_dict = json.loads(content)
            statuses_count = content_dict.get('data').get('userInfo')["statuses_count"]
            page_num = statuses_count // 10 + 1
            return page_num
        except IOError:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")  # , level=logging.ERROR)
            return 0
        except TypeError:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")  # , level=logging.ERROR)
            return 0

    # Override父类的make_request_from_data方法，解析json生成Request
    def make_request_from_data(self, data):
        data = json.loads(data)
        url = data.get('url')
        callback = data.get('callback')
        meta = data.get('meta')
        print("Fetch url:", url)
        logging.log(msg="Fetch url:" + url, level=logging.INFO)
        if callback == "parse_user":
            return scrapy.Request(url=url, callback=self.parse_user, meta=meta, dont_filter=True)
        elif callback == "parse_post":
            return scrapy.Request(url=url, callback=self.parse_post, dont_filter=True)

    def crawling_user_info(self, u):
        # to generate user's profile information url
        user_info_url = self.start_urls[0] + self.__user_info_api['api_0'] + \
                        u + self.__user_info_api['api_1'] + u
        # print(user_info_url)
        return user_info_url

    def crawling_post_info(self, u):
        # to generate user's tweet/post/weibo information url
        weibo_info_urls = []
        self.total_flag = 1
        for i in range(0, self.__weibo_page_range + 1):
            weibo_info_url = self.start_urls[0] + self.__weibo_info_api['api_0'] + u + \
                             self.__weibo_info_api['api_1'] + u + self.__weibo_info_api['api_2'] + str(i)
            # print(weibo_info_url)
            weibo_info_urls.append(weibo_info_url)
        return weibo_info_urls

    def parse_user(self, response):
        # the parser for user profile
        user_info = json.loads(response.text)['data']['userInfo']
        del user_info['toolbar_menus']
        # crawl the total number of this user
        # total_item = TotalNumItem()
        # total_item['uid'] = self.__uid
        # total_item['total_num'] = user_info['statuses_count']  # total number of user posts
        # yield total_item
        user_info_item = UserInfoItem()
        # user_info_item['user_info'] = user_info
        user_info_item['dataType'] = 0
        user_info_item['uu_id'] = self.__task_id
        user_info_item['uid'] = user_info['id']
        user_info_item['screen_name'] = user_info['screen_name']
        user_info_item['description'] = user_info['description']
        user_info_item['avatar_hd'] = user_info['avatar_hd']
        user_info_item['follow_count'] = user_info['follow_count']
        user_info_item['followers_count'] = user_info['followers_count']
        user_info_item['post_count'] = user_info['statuses_count']
        user_info_item['gender'] = user_info['gender']
        user_info_item['verified'] = user_info['verified']
        user_info_item['verified_reason'] = user_info['verified_reason']
        yield user_info_item

    def parse_post(self, response):
        # the parser for user post
        weibo_info = json.loads(response.text)
        cardListInfo = weibo_info['data']['cardlistInfo']
        # # crawl the total number of this user
        # total_item = TotalNumItem()
        # total_item['uid'] = self.__uid
        # total_item['total_num'] = cardListInfo['total']  # total number of user posts
        # yield total_item
        for card in weibo_info['data']['cards']:
            if card['card_type'] == 9:
                # only card_type equals 9, we need
                mblog = card['mblog']
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
                    logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [WeiboSpider] ")
                                    + "WeiboSpider" + ": failed to upload image:"
                                    + file_name, level=logging.INFO)
                user_post_item['pics'][i] = '/data/' + file_name
        else:
            user_post_item['pics'] = None
        #  下载视频
        if self.crawl_video:
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
                    logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [WeiboSpider] ")
                                    + "WeiboSpider" + ": failed to upload video:"
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
            yield item

    def parse_longtext(self, response):
        # parser for longtext post
        user_post_item = response.meta['post_item']
        try:
            data = json.loads(response.text)['data']
            user_post_item['text'] = data['longTextContent']
            item = self.parse_field(user_post_item)
        except:
            # item = None
            return

        yield item

    def parse_field(self, item):
        user_post_item = UserPostItem()
        user_post_item['dataType'] = 1
        user_post_item['uu_id'] = self.__task_id
        user_post_item['mid'] = item['mid']
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

    def fileUpload(self, file_path, file_name):
        upload_url = 'http://192.168.0.230:8888/upload'
        header = {"ct": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}
        files = {'file': open(file_path, 'rb')}
        # upload_data = {"parentId": "", "fileCategory": "personal", "fileSize": 179, "fileName": file_name, "uoType": 1}
        upload_data = {"fileName": file_name}
        upload_res = requests.post(upload_url, upload_data, files=files, headers=header)
        return upload_res.text

    def get_important_user(self):
        db_connector = DBConnector()
        db, client = db_connector.create_mongo_connection()
        l = db['user'].find({}, {"uid": 1})
        uid_list = []
        for item in l:
            uid_list.append(item['uid'])
        return uid_list
