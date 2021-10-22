# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

import os
import re
import time
import json
import random
import logging
import subprocess
from fake_useragent import UserAgent
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
import pymongo
import urllib
from twisted.internet.error import TCPTimedOutError, TimeoutError
from scrapy.core.downloader.handlers.http11 import TunnelError
from twisted.internet.error import ConnectionRefusedError

class Open_IP_Info:
    def __init__(self, ip_port: str, http_type: str, isp: str, location: str, ping_time: str, transfer_time: str,
                 check_dtime: str, effectiveness: int):
        self.ip_port = ip_port
        self.http_type = http_type
        self.isp = isp
        self.location = location
        self.ping_time = ping_time
        self.transfer_time = transfer_time
        self.check_dtime = check_dtime
        self.effectiveness = effectiveness

# to add random user-agent for every request
# to add random proxy IP address for every request
class RandomUaAndProxyIpMiddleware(UserAgentMiddleware):
    def __init__(self, ua, ip_num, api):
        super(UserAgentMiddleware, self).__init__()
        self.ua = ua
        self.api = api
        self.ip_num = ip_num

    @classmethod
    def from_crawler(cls, crawler):
        api = crawler.settings.get('PROXY_API')  # api to get proxy ip address, usually an url
        ip_num = int(re.findall(r'count=\d+', api)[0][6:])  # number of the proxy ip getting from url
        s = cls(ua=UserAgent(), ip_num=ip_num, api=api)
        return s

    def process_request(self, request, spider):
        proxy_list = get_proxy_ip_list()  #proxy_list
        proxy = random.choice(proxy_list)
        task_id = spider.middle
        print(task_id)
        print(proxy)
        if find_protocol_type(proxy) == 'Socks4':
            print("This proxy uses Socks4!")
            new_proxy = find_socks_proxy(proxy, task_id)
            if new_proxy.split(":",1)[0] == '127.0.0.1':
                request.meta['proxy'] = 'http://' + new_proxy
                request.headers['User-agent'] = self.ua.random
            else:
                new_proxy = Socks4_Process(proxy, task_id)
                request.meta['proxy'] = 'http://' + new_proxy
                request.headers['User-agent'] = self.ua.random
        elif find_protocol_type(proxy) == 'Socks5'or find_protocol_type(proxy) == 'Socks4/Socks5':
            print("This proxy uses Socks5!")
            new_proxy = find_socks_proxy(proxy, task_id)
            if new_proxy.split(":", 1)[0] == '127.0.0.1':
                request.meta['proxy'] = 'http://' + new_proxy
                request.headers['User-agent'] = self.ua.random
            else:
                new_proxy = Socks5_Process(proxy, task_id)
                request.meta['proxy'] = 'http://' + new_proxy
                request.headers['User-agent'] = self.ua.random
        elif find_protocol_type(proxy) == 'HTTPS':
            request.meta['proxy'] = 'https://' + proxy
            request.headers['User-agent'] = self.ua.random
        elif find_protocol_type(proxy) == 'HTTP':
            request.meta['proxy'] = 'http://' + proxy
            request.headers['User-agent'] = self.ua.random
        else:
            request.meta['proxy'] = 'http://' + proxy
            request.headers['User-agent'] = self.ua.random
        # print(request.meta)
        print(request.meta['proxy'])
        print(request.headers['User-agent'])
        print("                           ")

    #代理IP发送请求后可能有不少网络异常，抛出几种常见异常，出现异常就换一个代理IP+重新请求
    def process_exception(self, request, exception, spider):
        if isinstance(exception, TimeoutError):
            print("TimeoutError!")
            self.process_request_back(request, spider)
            return request

        elif isinstance(exception, TCPTimedOutError):
            print("TCPTimeOutError!")
            self.process_request_back(request, spider)
            return request

        elif isinstance(exception, TunnelError):
            print("TunnelError!")
            self.process_request_back(request, spider)
            return request

        elif isinstance(exception, ConnectionRefusedError):
            print("ConnectionRefusedError")
            self.process_request_back(request, spider)
            return request
        else:
            print("This Proxy is WRONG! Change Another one!")
            self.process_request_back(request, spider)
            return request

# request.headers['User-agent'] = self.ua.random
# request.meta['proxy'] = 'http://45.244.148.15:999'
    def process_request_back(self, request, spider):
        print("Process Request should be changed!")
        task_id = spider.middle
        proxy_list = get_proxy_ip_list()
        proxy = request.meta['proxy'].split("//", 1)[1]  # 考虑一下字符串分割
        if proxy.split(":",1)[0] == '127.0.0.1':
            #此时代理ip需要映射回原始代理ip字段,如果原代理ip已移动到已删除代理ip池中，则更换当前代理ip
            old_proxy = find_old_ip(proxy, task_id)
            #这里存在可能原始代理ip字段已经删除的情况！
            if old_proxy == False:
                proxy = random.choice(get_proxy_ip_list())
            else:
                proxy = proxy_effectiveness_update(old_proxy, proxy_list)
        else:
            # 这里存在可能原始代理ip字段已经删除的情况！要进行检验；
            proxy_existence = check_proxy_existence(proxy)
            if proxy_existence:
                proxy = proxy_effectiveness_update(proxy, proxy_list)
            else:
                proxy = random.choice(get_proxy_ip_list())
        request.headers['User-agent'] = self.ua.random
        if find_protocol_type(proxy) == 'Socks4':
            print("This proxy uses Socks4!")
            new_proxy = find_socks_proxy(proxy, task_id)
            if new_proxy.split(":", 1)[0] == '127.0.0.1':
                request.meta['proxy'] = 'http://' + new_proxy
            else:
                new_proxy = Socks4_Process(proxy, task_id)
                request.meta['proxy'] = 'http://' + new_proxy
        elif find_protocol_type(proxy) == 'Socks5' or find_protocol_type(proxy) == 'Socks4/Socks5':
            print("This proxy uses Socks5!")
            new_proxy = find_socks_proxy(proxy, task_id)
            if new_proxy.split(":", 1)[0] == '127.0.0.1':
                request.meta['proxy'] = 'http://' + new_proxy
            else:
                new_proxy = Socks5_Process(proxy, task_id)
                request.meta['proxy'] = 'http://' + new_proxy
        elif find_protocol_type(proxy) == 'HTTPS':
            request.meta['proxy'] = 'https://' + proxy
        elif find_protocol_type(proxy) == 'HTTP':
            request.meta['proxy'] = 'http://' + proxy
        else:
            request.meta['proxy'] = 'http://' + proxy

# to solve crawling failed
class RetryMiddleware(object):

    def __init__(self, ip_num, retry_time=3):
        self.retry_time = retry_time
        self.ua = UserAgent()
        self.__err_count = {}  # request error times
        self.ip_num = ip_num

    @classmethod
    def from_crawler(cls, crawler):
        api = crawler.settings.get('PROXY_API')
        # print(api)
        ip_num = int(re.findall(r'count=\d+', api)[0][6:])
        s = cls(ip_num=ip_num)
        return s

    def process_response(self, request, response, spider):
        #print(request.meta)
        print("Process Response:")
        print(response.status)
        print("                     ")
        task_id = spider.middle
        if response.status == 418:
            # receive http status code 418, resend this request
            url_hash = hash(request.url)
            # to count the recrawling times for each request
            if url_hash not in self.__err_count.keys():
                self.__err_count[url_hash] = 0
            else:
                self.__err_count[url_hash] += 1
            # 出现一次418，有效性分数就减少5分
            proxy_list = get_proxy_ip_list()
            #print(len(proxy_list))
            proxy = request.meta['proxy'].split("//", 1)[1] #考虑一下字符串分割
            print(proxy)
            if proxy.split(":", 1)[0] == '127.0.0.1':
                # 此时代理ip需要映射回原始代理ip字段
                old_proxy = find_old_ip(proxy, task_id)
                proxy = proxy_effectiveness_update(old_proxy, proxy_list)
            else:
                proxy = proxy_effectiveness_update(proxy, proxy_list)
            # to resend this request and change the ua and proxy ip
            if self.__err_count[url_hash] < self.retry_time:
                request.headers['User-agent'] = self.ua.random
                if find_protocol_type(proxy) == 'Socks4':
                    print("This proxy uses Socks4!")
                    new_proxy = find_socks_proxy(proxy, task_id)
                    if new_proxy.split(":", 1)[0] == '127.0.0.1':
                        request.meta['proxy'] = 'http://' + new_proxy
                    else:
                        new_proxy = Socks4_Process(proxy, task_id)
                        request.meta['proxy'] = 'http://' + new_proxy
                elif find_protocol_type(proxy) == 'Socks5' or find_protocol_type(proxy) == 'Socks4/Socks5':
                    print("This proxy uses Socks5!")
                    new_proxy = find_socks_proxy(proxy, task_id)
                    if new_proxy.split(":", 1)[0] == '127.0.0.1':
                        request.meta['proxy'] = 'http://' + new_proxy
                    else:
                        new_proxy = Socks5_Process(proxy, task_id)
                        request.meta['proxy'] = 'http://' + new_proxy
                elif find_protocol_type(proxy) == 'HTTPS':
                    request.meta['proxy'] = 'https://' + proxy
                elif find_protocol_type(proxy) == 'HTTP':
                    request.meta['proxy'] = 'http://' + proxy
                else:
                    request.meta['proxy'] = 'http://' + proxy
                # add proxy for the new request
                # proxy = RandomUaAndProxyIpMiddleware.get_proxy_ip(self.ip_num)
                # request.meta['proxy'] = proxy
                logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [RetryMiddleware] ")
                                + spider.name + ": restart crawl url:" + response.url, level=logging.INFO)
                return request
            else:
                # raise error IgnoreRequest to drop this request
                logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [RetryMiddleware] ")
                                + spider.name + ": drop request by maximum retry, url:" + response.url,
                            level=logging.INFO)
                raise IgnoreRequest
        else:
            try:
                parse_json = json.loads(response.text)
                if parse_json['ok'] == 0:
                    # crawl empty json string
                    # drop this request
                    logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [RetryMiddleware] ")
                                    + spider.name + ": drop request by empty json, url:" + response.url,
                                level=logging.INFO)
                    raise IgnoreRequest
                else:
                    # request.meta['parse_json'] = parse_json
                    return response
            except json.JSONDecodeError:
                # error when json string decoding, drop this request
                if "<!DOCTYPE html>" in response.text:
                    # crawled string is a html file
                    return response
                else:
                    logging.log(msg=time.strftime("%Y-%m-%d %H:%M:%S [RetryMiddleware] ")
                                    + spider.name + ": drop request by json decoding error, url:"
                                    + response.url, level=logging.INFO)
                    raise IgnoreRequest


#根据米扑代理API获取生成代理IP，写入MongoDB中
def generate_proxy_ip():
    proxy_url = 'https://proxyapi.mimvp.com/api/fetchopen?num=100&orderid=866050700105144909&http_type=0' \
                '&result_fields=1,2,4,5,6,7,9&result_format_need_request=3'  #http_type=0 all;1 http; 2 https
    req = urllib.request.Request(proxy_url)
    content = urllib.request.urlopen(req, timeout=60).read()
    proxy_list = content.decode().split("\n")
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]
    existing_num = mycol.count()
    print("Generating proxy ip!")
    for proxy in proxy_list:
        proxy_info_list = proxy.split(",")
        ip_info = Open_IP_Info(proxy_info_list[0], proxy_info_list[1], proxy_info_list[2], proxy_info_list[3],
                                proxy_info_list[4], proxy_info_list[5], proxy_info_list[6], 100)
        mydict = {"ip:port": ip_info.ip_port, "http_type": ip_info.http_type, "isp": ip_info.isp,
                    "location": ip_info.location, "ping_time": ip_info.ping_time,
                    "transfer_time": ip_info.transfer_time, "check_dtime": ip_info.check_dtime,
                    "effectiveness": ip_info.effectiveness}
        myquery = {"ip:port": ip_info.ip_port}
        query = mycol.find_one(myquery)
        if not query:
            if existing_num >= 60:  #这里的60是一个阈值，生成的代理池数量维持在阈值以下，不够再补充
                break
            else:
                x = mycol.insert_one(mydict)
                print("New proxy ip has been saved in the proxy_ip_pool!")
                print(x)
                existing_num = mycol.count()
        else:
            print("This proxy ip has existed in our database.")

def get_proxy_ip_list():
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]
    proxy_ip_list = []
    for x in mycol.find():
        proxy_ip_list.append(x["ip:port"])
    return proxy_ip_list

def proxy_effectiveness_update(proxy_port, proxy_list):
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]
    decol = mydb["deleted_proxy_ip_pool"]
    x = mycol.find_one({"ip:port": proxy_port})
    if x == None:
        print("Proxy has been invalid. Find it in the deleted proxy list!")
        proxy = random.choice(get_proxy_ip_list())
        return proxy
    else:
        mydict = {"ip:port": x["ip:port"], "http_type": x["http_type"], "isp": x["isp"],
                  "location": x["location"], "ping_time": x["ping_time"],
                  "transfer_time": x["transfer_time"], "check_dtime": x["check_dtime"],
                  "effectiveness": x["effectiveness"]}
        effectiveness = x["effectiveness"]
        new_effectiveness = effectiveness - 5
        mydict["effectiveness"] = new_effectiveness
        if new_effectiveness < 60:
            mycol.delete_one({"ip:port": proxy_port})
            proxy_list.remove(proxy_port)
            # 由于api接口访问请求有间隔时间限制，所以需要加入定时暂停挂起，目前由于该免费api接口返回的代理基本用不了，所以暂时注释掉
            # time.sleep(10)
            # generate_proxy_ip()
            proxy = random.choice(proxy_list)
            if len(proxy_list) < 5:
                print("Waring! Should ADD NEW proxy!")
            mydict["effectiveness"] = new_effectiveness
            decol.insert_one(mydict)  # 原来被删除的代理ip放到另一个数据库里，为后续重加入开发做准备工作
            return proxy
        else:
            newvalues = {'$set': {"effectiveness": new_effectiveness}}
            mycol.update_one({"ip:port": proxy_port}, newvalues)
            proxy = random.choice(proxy_list)
            return proxy


def find_protocol_type(ip_port):
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]  #proxy_ip_pool
    protocol_type = mycol.find_one({"ip:port": ip_port})["http_type"]
    #protocol_type = mycol.find_one({"ip:port": ip_port})
    return protocol_type

def updateDB_port(old_proxy, new_proxy, task_id, task_pid):
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]
    # Socks相关代理映射存储在task_id字段
    newvalues = {'$set': {task_id: new_proxy}}
    mycol.update_one({"ip:port": old_proxy}, newvalues)
    #存储该task下socks端口转换的相关pid
    col = mydb["task_socks_pid"]
    TASK_PID = 'Socks_pid'
    query = col.find_one({"Task_id": task_id})
    if query == None:
        # 该task的pid存储在TASK_PID字段
        TASK_PID_Content = f'{task_pid}'
        mydict = {"Task_id":task_id, TASK_PID:TASK_PID_Content}
        col.insert_one(mydict)
    else:
        TASK_PID_Content = query[TASK_PID]
        TASK_PID_Content = f'{TASK_PID_Content},{task_pid}'
        pid_newvalues = {'$set': {TASK_PID: TASK_PID_Content}}
        col.update_one({"Task_id": task_id}, pid_newvalues)

def find_socks_proxy(ip_port, task_id):
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]  #proxy_ip_pool
    socks_proxy = mycol.find_one({"ip:port": ip_port})
    if task_id in socks_proxy:
        socks_proxy = socks_proxy[task_id]
        return socks_proxy
    else:
        return ''

def find_old_ip(socks_proxy, task_id):
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]  #proxy_ip_pool
    try:
        old_proxy = mycol.find_one({task_id: socks_proxy})["ip:port"]
    except:
        print("Proxy has been invalid. Find it in the deleted proxy list!")
        return False
    return old_proxy

def check_proxy_existence(proxy):
    myclient = pymongo.MongoClient("mongodb://139.9.205.93:27019")
    mydb = myclient["proxy_ip"]
    mycol = mydb["proxy_ip_pool"]  # proxy_ip_pool
    try:
        query = mycol.find_one({"ip:port": proxy})
    except:
        print("Proxy has been invalid. Find it in the deleted proxy list!")
        return False
    return True

def Socks4_Process(proxy, task_id):
    port = proxy.split(":", 1)[1]
    # ip = proxy.split(":",1)[0]
    localhost = '127.0.0.1'
    flag = True
    x = random.choice(range(200))
    while flag:  
        #检查此时端口号x是否已被使用
        port = int(port) + x
        port_check_cmd = 'lsof -i'
        port_cmd = f'{port_check_cmd}:{port}'
        port_subp = subprocess.Popen(port_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            encoding='utf-8')
        if port_subp.returncode == None:
            flag = False
        else:
            x = random.choice(range(200))
    cmd1 = 'pproxy -l http://'
    cmd2 = '-r socks4://'
    cmd3 = '-vv'
    cmd = f'{cmd1}:{port} {cmd2}{proxy} {cmd3}'
    #print(cmd)
    subp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            encoding='utf-8')
    # time.sleep(2)
    if subp.returncode == None:
        print("Socks4->HTTP success!")
    else:
        print("error:", subp)
    task_pid = subp.pid
    new_port = str(port)
    new_proxy = f'{localhost}:{new_port}'
    updateDB_port(proxy, new_proxy, task_id, task_pid)
    return new_proxy

def Socks5_Process(proxy, task_id):
    print("Socks5_Process:")
    print(proxy)
    port = proxy.split(":", 1)[1]
    # ip = proxy.split(":",1)[0]
    localhost = '127.0.0.1'
    flag = True
    x = random.choice(range(200))
    while flag:
        # 检查此时端口号x是否已被使用
        port = int(port) + x
        port_check_cmd = 'lsof -i'
        port_cmd = f'{port_check_cmd}:{port}'
        port_subp = subprocess.Popen(port_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     encoding='utf-8')
        if port_subp.returncode == None:
            flag = False
        else:
            x = random.choice(range(200))
    cmd1 = 'pproxy -l http://'
    cmd2 = '-r socks5://'
    cmd3 = '-vv'
    cmd = f'{cmd1}:{port} {cmd2}{proxy} {cmd3}'
    print(cmd)
    subp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            encoding='utf-8')
    # time.sleep(2)
    if subp.returncode == None:
        print("Socks5->HTTP success!")
    else:
        print("error:", subp)
    task_pid = subp.pid
    new_port = str(port)
    new_proxy = f'{localhost}:{new_port}'
    updateDB_port(proxy, new_proxy, task_id, task_pid)
    return new_proxy
