# coding:utf-8
# 系统模块
import datetime
import time
import socket
from scrapyd_api import ScrapydAPI

# 自定义模块
from SpiderKeeper.app.proxy.spiderctrl import SpiderServiceProxy
from SpiderKeeper.app.spider.model import SpiderStatus, Project, SpiderInstance

from SpiderKeeper.app.util.http import request
import requests


class ScrapydProxy(SpiderServiceProxy):
    '''
    单个爬虫服务类
    继承单个爬虫服务基类, 实现基类的功能
    '''
    def __init__(self, server):
        self.spider_status_name_dict = {
            SpiderStatus.PENDING: 'pending',
            SpiderStatus.RUNNING: 'running',
            SpiderStatus.FINISHED: 'finished'
        }
        super(ScrapydProxy, self).__init__(server)  # super执行的是父类的方法
        self.scrapyd_api = ScrapydAPI(self._scrapyd_url())   # 实例化ScrapydAPI

    def _scrapyd_url(self):
        return self.server  # 得到scrapyd的url, 用到实现的get方法

    def list_projects(self):
        """
        获取指定scrapyd上的所有工程列表,返回工程名字符串列表,而get_project_list返回的是对象
        :return:
        """
        # 获取scrapyd上的所有工程列表
        return self.scrapyd_api.list_projects()

    def get_project_list(self):
        """
        功能: 获取所有的爬虫工程列表
        :return: 返回工程对象列表
        """
        data = self.scrapyd_api.list_projects()  # 获取scrapyd上的所有工程列表
        result = []
        if data:
            for project_name in data:
                project = Project()  # 实例化工程对象
                project.project_name = project_name
                result.append(project)
        return result

    def delete_project(self, project_name):
        """
        功能: scrapyd上删除指定工程
        :param project_name: 工程名称
        :return:
        """
        try:
            return self.scrapyd_api.delete_project(project_name)  # 返回状态, 工程存在, 删除后返回True
        except:
            return False

    def get_slave_spider_list(self, project_name):
        try:
            data = self.scrapyd_api.list_spiders(project_name)  # 列出指定工程下所有的爬虫名称
            return data if data else []
        except:
            return []

    def get_spider_list(self, project_name):
        """
        功能: 获取指定工程下的所有爬虫名称列表
        :param project_name: 工程名称
        :return: 返回爬虫实例对象列表
        """
        try:
            data = self.scrapyd_api.list_spiders(project_name)  # 列出指定工程下所有的爬虫名称
            result = []
            if data:
                for spider_name in data:
                    spider_instance = SpiderInstance()
                    spider_instance.spider_name = spider_name
                    result.append(spider_instance)
            return result
        except:
            return []

    def get_daemon_status(self):
        pass

    def get_job_list(self, project_name, spider_status=None):
        """
        从scrapyd中获取一个爬虫项目下面的所有蜘蛛任务状态
        :param project_name: 爬虫项目名称
        :param spider_status:  蜘蛛状态, 默认为None, 返回所有状态, 若传入状态值, 则返回某个状态
        :return:
        """
        result = {SpiderStatus.PENDING: [], SpiderStatus.RUNNING: [], SpiderStatus.FINISHED: []}
        try:
            data = self.scrapyd_api.list_jobs(project_name)
            if data:
                for _status in self.spider_status_name_dict.keys():
                    for item in data[self.spider_status_name_dict[_status]]:
                        start_time, end_time = None, None
                        if item.get('start_time'):
                            start_time = datetime.datetime.strptime(item['start_time'], '%Y-%m-%d %H:%M:%S.%f')
                        if item.get('end_time'):
                            end_time = datetime.datetime.strptime(item['end_time'], '%Y-%m-%d %H:%M:%S.%f')
                        result[_status].append(dict(id=item['id'], start_time=start_time, end_time=end_time))
            return result if not spider_status else result[spider_status]
        except:
            return result

    def start_spider(self, project_name, spider_name):
        """
        功能：启动指定工程下的指定爬虫
        :param project_name: 工程名称
        :param spider_name: 爬虫名称
        :return: 返回启动的爬虫的id, 启动不成功, 返回None
        """
        data = self.scrapyd_api.schedule(project_name, spider_name)
        return data if data else None

    def cancel_spider(self, project_name, job_id):
        """
        功能: 取消工程下的指定job
        :param project_name: 工程名称 str
        :param job_id: job_id str
        :return: 成功取消, 返回True, 否则返回False
        """
        data = self.scrapyd_api.cancel(project_name, job_id)
        return data != None

    def deploy(self, project_name, file_path):
        """
        功能: 将上传的egg项目部署到scrapyd上
        :param project_name: 工程名称 str
        :param file_path: egg文件路径 str
        :return: 成功返回字典型工程信息, 否则返回None
        """
        egg = open(file_path, 'rb')
        version = int(time.time())
        spider_num = self.scrapyd_api.add_version(project_name, int(time.time()), egg)
        egg.close()
        ret = {
            'version': version,
            'project': project_name,
            'spiders': spider_num,
            'node_name': socket.gethostname(),
            'status': 'ok' if spider_num else 'error'
        }
        return str(ret) if spider_num else False

    def log_url(self, project_name, spider_name, job_id):
        """
        功能: 获取爬虫的日志
        :param project_name: 工程名称 str
        :param spider_name: 爬虫名称 str
        :param job_id: job_id str
        :return: 返回log日志文件的url str
        """
        return self._scrapyd_url() + '/logs/%s/%s/%s.log' % (project_name, spider_name, job_id)
