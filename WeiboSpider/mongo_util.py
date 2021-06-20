import os
from .database_tool import DBConnector
import gridfs
import requests
from urllib.request import urlretrieve


class mongo_util(object):

    def __init__(self):
        if not os.path.exists('/data/'):
            os.makedirs('/data/')
        db_connector = DBConnector()
        self.db, self.client = db_connector.create_mongo_connection()
        self.__imgfs = gridfs.GridFS(self.db, "img")
        self.__videofs = gridfs.GridFS(self.db, "video")

    def save_image(self, url, file_name):
        """下载图片到本地后上传到MongoDB Gridfs后删除本地文件"""
        urlretrieve(url, '/data/' + file_name)  # 将图片下载到本地
        with open('/data/' + file_name, 'rb') as my_image:
            picture_data = my_image.read()
            file_ = self.__imgfs.put(data=picture_data, filename=file_name)  # 上传到gridfs
            # print(file_)
        os.remove('/data/' + file_name)  # 删除本地图片

    def save_video(self, url, file_name):
        res = requests.get(url, stream=True)
        with open('/data/' + file_name, "wb") as mp4:
            for chunk in res.iter_content(
                    chunk_size=1024 * 1024):
                if chunk:
                    mp4.write(chunk)
        with open('/data/' + file_name, 'rb') as my_video:
            video_data = my_video.read()
            file_ = self.__videofs.put(data=video_data, filename=file_name)
            # print(file_)
        os.remove('/data/' + file_name)
