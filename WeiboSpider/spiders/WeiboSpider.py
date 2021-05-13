import os
import re
import json
import time

import scrapy
import logging
import redis
import requests
from lxml import etree
from ..items import *
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from urllib.request import urlretrieve


class WeiboSpider(RedisSpider):
    # init parameters
    name = 'WeiboSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']  # crawling sites
    handle_httpstatus_list = [418]  # http status code for not ignoring
    redis_key = 'WeiboSpider:start_urls'

    def __init__(self, uid, node='master', task_id='1996', page=199, *args, **kwargs):
        super(WeiboSpider, self).__init__(*args, **kwargs)
        self.start_urls = ['https://m.weibo.cn/']
        self.__uid = uid
        self.__task_id = task_id
        self.__user_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=', 'api_1': '&containerid=100505'}
        self.__weibo_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=',
                                 'api_1': '&containerid=107603', 'api_2': '&page=',
                                 'longtext_api': 'https://m.weibo.cn/statuses/extend?id=',
                                 'precise_time_api': 'https://m.weibo.cn/status/'}
        self.__weibo_page_range = int(page)
        self.redis_key = self.redis_key+task_id

        if node == 'master':
            # 获取redis连接
            # pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
            # r = redis.Redis(connection_pool=pool)
            settings = get_project_settings()
            r = redis.Redis(host=settings.get("REDIS_HOST"), port=settings.get("REDIS_PORT"), decode_responses=True)

            # 向Redis存入初始请求
            user_info_url = self.crawling_user_info()  # 拼接用户信息URL
            # 获取总页数
            page_num = self.parse_page_num(user_info_url)
            print(page_num, page)
            self.__weibo_page_range = min(page_num, int(page))
            # r.lpush(self.redis_key, user_info_url)
            request_data = {
                'url': user_info_url,
                'callback': "parse_user",
                'meta': {'repeat_times': 0}
            }
            r.lpush(self.redis_key, json.dumps(request_data))

            weibo_info_urls = self.crawling_post_info()  # 拼接博文URL
            for weibo_info_url in weibo_info_urls:
                # r.lpush(self.redis_key, user_info_url)
                request_data = {
                    'url': weibo_info_url,
                    'callback': "parse_post",
                    'meta': None
                }
                r.lpush(self.redis_key, json.dumps(request_data))
            # time.sleep(2)

    def parse_page_num(self, url):
        try:
            content = requests.get(url).text
        except IOError:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")  # , level=logging.ERROR)
            return 3
        if 'data' not in content:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")  # , level=logging.ERROR)
            return 3
        content_dict = json.loads(content)
        statuses_count = content_dict.get('data').get('userInfo')["statuses_count"]
        page_num = statuses_count // 10 + 1
        return page_num

    # Override父类的make_request_from_data方法，解析json生成Request
    def make_request_from_data(self, data):
        data = json.loads(data)
        url = data.get('url')
        callback = data.get('callback')
        meta = data.get('meta')
        print("Fetch url:", url)
        if callback == "parse_user":
            return scrapy.Request(url=url, callback=self.parse_user, meta=meta, dont_filter=True)
        elif callback == "parse_post":
            return scrapy.Request(url=url, callback=self.parse_post, dont_filter=True)

    def crawling_user_info(self):
        # to generate user's profile information url
        user_info_url = self.start_urls[0] + self.__user_info_api['api_0'] + \
                        self.__uid + self.__user_info_api['api_1'] + self.__uid
        # print(user_info_url)
        return user_info_url

    def crawling_post_info(self):
        # to generate user's tweet/post/weibo information url
        weibo_info_urls = []
        self.total_flag = 1
        for i in range(0, self.__weibo_page_range + 1):
            weibo_info_url = self.start_urls[0] + self.__weibo_info_api['api_0'] + self.__uid + \
                             self.__weibo_info_api['api_1'] + self.__uid + self.__weibo_info_api['api_2'] + str(i)
            # print(weibo_info_url)
            weibo_info_urls.append(weibo_info_url)
        return weibo_info_urls

    def parse_user(self, response):
        # the parser for user profile
        user_info = json.loads(response.text)['data']['userInfo']
        del user_info['toolbar_menus']
        # crawl the total number of this user
        total_item = TotalNumItem()
        total_item['uid'] = self.__uid
        total_item['total_num'] = user_info['statuses_count']  # total number of user posts
        yield total_item
        user_info_item = UserInfoItem()
        # user_info_item['user_info'] = user_info
        user_info_item['task_id'] = self.__task_id
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
                # 爬取图片
                for i in range(min(9, mblog['pic_num'])):
                    pic = mblog['pics'][i]
                    pic_url = 'https://wx3.sinaimg.cn/large/'+pic['pid']+'.jpg'
                    # pic_url = pic['url']
                    # urlretrieve(pic_url, './img/posts/' + self.__task_id + '_' + mblog['mid'] + '_' + str(i) + '.jpg')
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
                    # with open('./video/' + self.__task_id + '_' + mblog['mid']+'.mp4', "wb") as mp4:
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
                    # precise_time_url = self.__weibo_info_api['precise_time_api'] + mblog['id']
                    # yield scrapy.Request(url=precise_time_url, callback=self.parse_precise_time,
                    #                      meta={'post_item': mblog})

    def parse_longtext(self, response):
        # parser for longtext post
        user_post_item = response.meta['post_item']
        data = json.loads(response.text)['data']
        user_post_item['text'] = data['longTextContent']
        item = self.parse_field(user_post_item)
        yield item

    # def get_precise_time(self, text):
    #     page_text = etree.HTML(text)
    #     result = page_text.xpath('/html/body/script[1]/text()')
    #     time_str = re.findall(r'"created_at":.+"', "".join(result))
    #     if time_str:
    #         precise_time = json.loads('{' + time_str[0] + '}')['created_at']
    #     else:
    #         precise_time = None
    #     return precise_time
    #
    # def parse_precise_time(self, response):
    #     # parse for precise time
    #     try:
    #         user_post_item = response.meta['post_item']
    #         precise_time = self.get_precise_time(response.text)
    #         user_post_item['user_post']['precise_time'] = precise_time
    #         yield user_post_item
    #     except Exception as e:
    #         self.logger.info(message="[weibo_info_spider] parse_precise_time error!" + repr(e), level=logging.ERROR)

    def parse_field(self, item):
        user_post_item = UserPostItem()
        user_post_item['task_id'] = self.__task_id
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

    # def parse_comments(self, response):
    #     content = json.loads(response.text)
    #     max_id = content['data']['max_id']
    #     comments = content['data']['data']
    #     for comment in comments:
    #         comment_item = CommentItem()
    #         comment_item['comment'] = comment
    #         yield comment_item
