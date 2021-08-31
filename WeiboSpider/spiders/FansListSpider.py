import json
import requests
import logging
import scrapy
import redis
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from ..items import *
from distutils.util import strtobool
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class FansListSpider(RedisSpider):
    name = 'FansListSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']
    handle_httpstatus_list = [418]
    redis_key = 'FansListSpider:start_urls'

    def __init__(self, uid, node='master', uu_id='test', fans_end=50, follows_end=50, schedule='False', *args, **kwargs):
        super(FansListSpider, self).__init__(*args, **kwargs)
        # self.uid = uid
        self.__uid_list = list(filter(None, uid.split('|')))
        self.__task_id = uu_id
        self.start_urls = ['https://m.weibo.cn/']
        self.root_url = 'https://m.weibo.cn/'
        self.api = {'common_api': 'api/container/getIndex?containerid=231051', 'fans_api_0': '_-_fans_-_',
                    'fans_api_1': '&since_id=', 'follows_api_0': '_-_followers_-_', 'follows_api_1': '&page='}

        self.__user_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=', 'api_1': '&containerid=100505'}
        self.redis_key = self.redis_key + uu_id
        self.schedule = strtobool(schedule)

        if node == 'master':
            settings = get_project_settings()
            r = redis.Redis(host=settings.get("REDIS_HOST"), port=settings.get("REDIS_PORT"), password=settings.get("REDIS_PARAMS")['password'], decode_responses=True)
            for u in self.__uid_list:
                user_info_url = self.crawling_user_info(u)  # 拼接用户信息URL
                # 获取总页数
                followers_page_num, follow_page_num = self.parse_page_num(user_info_url)
                followers_page_num = min(followers_page_num, int(fans_end))
                follow_page_num = min(follow_page_num, int(follows_end))
                print("followers_page_num", followers_page_num)
                print("follow_page_num", follow_page_num)
                self.page_range = {'fans': {'start': 1, 'end': followers_page_num}, 'follows': {'start': 1, 'end': follow_page_num}}

                # 向Redis存入初始请求
                fans_url, follows_url = self.crawl_one(uid)
                for url in fans_url:
                    request_data = {
                        'url': url,
                        'callback': "parse_fans",
                        'meta': {'__uid': uid}
                    }
                    r.lpush(self.redis_key, json.dumps(request_data))
                    # yield scrapy.Request(url=url, callback=self.parse_fans, meta={'__uid': uid})
                for url in follows_url:
                    request_data = {
                        'url': url,
                        'callback': "parse_follows",
                        'meta': {'__uid': uid}
                    }
                    r.lpush(self.redis_key, json.dumps(request_data))
                    # yield scrapy.Request(url=url, callback=self.parse_follows, meta={'__uid': uid})

    # Override父类的make_request_from_data方法，解析json生成Request
    def make_request_from_data(self, data):
        data = json.loads(data)
        url = data.get('url')
        callback = data.get('callback')
        meta = data.get('meta')
        print("Fetch url:", url)
        logging.log(msg="Fetch url:" + url, level=logging.INFO)
        if callback == "parse_fans":
            return scrapy.Request(url=url, callback=self.parse_fans, meta=meta, dont_filter=True)
        elif callback == "parse_follows":
            return scrapy.Request(url=url, callback=self.parse_follows, meta=meta, dont_filter=True)

    def parse_page_num(self, url):
        try:
            content = requests.get(url).text
        except IOError:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")  # , level=logging.ERROR)
            return 10, 10
        if 'data' not in content:
            self.logger.info(msg="[weibo_info_spider] parse_page_numm error!")# , level=logging.ERROR)
            return 10, 10
        content_dict = json.loads(content)
        followers_count = content_dict.get('data').get('userInfo')["followers_count"]
        follow_count = content_dict.get('data').get('userInfo')["follow_count"]
        followers_page_num = min(1000, followers_count)//10 + 1
        follow_page_num = min(1000, follow_count)//20 + 1
        return followers_page_num, follow_page_num

    def crawl_one(self, uid):
        fans_url_template = self.root_url + self.api['common_api'] + \
                            self.api['fans_api_0'] + uid + self.api['fans_api_1']
        follows_url_template = self.root_url + self.api['common_api'] + \
                               self.api['follows_api_0'] + uid + self.api['follows_api_1']
        fans_urls = [(fans_url_template + str(page_index)) for page_index in range(
            self.page_range['fans']['start'], self.page_range['fans']['end'] + 1)
        ]
        follows_url = [(follows_url_template + str(page_index)) for page_index in range(
            self.page_range['follows']['start'], self.page_range['follows']['end'] + 1
        )]
        return fans_urls, follows_url

    def crawling_user_info(self, u):
        # to generate user's profile information url
        user_info_url = self.start_urls[0] + self.__user_info_api['api_0'] + \
                        u + self.__user_info_api['api_1'] + u
        # print(user_info_url)
        return user_info_url

    def parse_fans(self, response):
        cards = json.loads(response.text)['data']['cards']
        fans_item = FansListItem()
        user_info_item = UserInfoItem()
        #  fans_item['uid'] = response.meta['__uid']
        fans_item['uu_id'] = self.__task_id
        # fans_list = []
        for crd in cards:
            if crd['card_type'] == 11:
                for card in crd['card_group']:
                    if card['card_type'] == 10:
                        # fans_item['fan'] = card['user']
                        # fans_item['uid'] = response.meta['__uid']
                        fans_item['dataType'] = 2
                        fans_item['s_id'] = card['user']['id']
                        fans_item['t_id'] = response.meta['__uid']
                        yield fans_item
                        user_info = card['user']
                        # user_info_item['user_info'] = user_info
                        user_info_item['dataType'] = 0
                        user_info_item['uu_id'] = self.__task_id
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
                        yield user_info_item
        # fans_item['fans_list'] = fans_list
        # yield fans_item

    def parse_follows(self, response):
        cards = json.loads(response.text)['data']['cards']
        fans_item = FansListItem()
        # fans_item['uid'] = response.meta['__uid']
        fans_item['uu_id'] = self.__task_id
        user_info_item = UserInfoItem()
        # follows_list = []
        for crd in cards:
            if crd['card_type'] == 11:
                for card in crd['card_group']:
                    if card['card_type'] == 10:
                        # follows_list.append([card['user']])
                        # follows_item['follower'] = card['user']
                        # follows_item['uid'] = response.meta['__uid']
                        fans_item['dataType'] = 2
                        fans_item['s_id'] = response.meta['__uid']
                        fans_item['t_id'] = card['user']['id']
                        yield fans_item
                        user_info = card['user']
                        # user_info_item['user_info'] = user_info
                        user_info_item['dataType'] = 0
                        user_info_item['uu_id'] = self.__task_id
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
                        yield user_info_item
                        # print(card['user'])
        # follows_item['follows_list'] = follows_list
        # yield follows_item
