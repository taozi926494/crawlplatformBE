import datetime
import random
from SpiderKeeper.app import db
from SpiderKeeper.app.spider.model import SpiderStatus, JobExecution, JobInstance, Project, JobPriority, SpiderInstance


class SpiderServiceProxy(object):
    """
    单个爬虫服务代理基类,
    这只是一个基类, 它的类方法是在继承它的类SpiderProxy中实现的
    """
    def __init__(self, server):
        self._server = server

    def get_project_list(self):
        pass

    def delete_project(self, project_name):
        pass

    def get_spider_list(self, *args, **kwargs):
        return NotImplementedError

    def get_daemon_status(self):
        return NotImplementedError

    def get_job_list(self, project_name, spider_status):
        return NotImplementedError

    def start_spider(self, *args, **kwargs):
        return NotImplementedError

    def cancel_spider(self, *args, **kwargs):
        return NotImplementedError

    def deploy(self, *args, **kwargs):
        pass

    def log_url(self, *args, **kwargs):
        pass

    @property
    def server(self):
        return self._server


class SpiderAgent():
    """
    爬虫代理服务类
    其实也就是把多个爬虫服务代理的实例统一做一遍轮询操作
    """
    def __init__(self):
        # 主爬虫服务实例列表
        self.spider_service_instances_master = []
        # 从爬虫服务实例列表
        self.spider_service_instances_slave = []

    def regist(self, spider_service_proxy, is_master):
        """
        注册爬虫代理
        :param spider_service_proxy:将单个爬虫服务代理append到多个爬虫服务代理列表里面
        :return:
        """
        # 判断两个实例是否是相同
        if isinstance(spider_service_proxy, SpiderServiceProxy):
            # 分别将相应的scrapyd实例添加到对应的服务器实例列表中
            if is_master == '1':
                # 将ScrapydProxy(server)添加到spider_service_instances_master列表中
                self.spider_service_instances_master.append(spider_service_proxy)
            else:
                # 将ScrapydProxy(server)添加到spider_service_instances_slave列表中
                self.spider_service_instances_slave.append(spider_service_proxy)

    def delete_project(self, project):
        """
        删除scrapyd上的工程
        :param project:
        :return:
        """
        if project.is_msd == '0':  # 如果是单机爬虫项目
            # 判断工程是否存在对应的爬虫服务器, 存在则删除, 目前单机爬虫部署在主爬虫服务器上
            for spider_service_instance in self.spider_service_instances_master:
                if project.project_name in spider_service_instance.list_projects():
                    return spider_service_instance.delete_project(project.project_name)
                return False
        else:
            del_spider_flag = True
            # 删除从服务器上某工程下的所有爬虫,所有都删除成功后才删除主服务器
            for spider_service_instance in self.spider_service_instances_slave:
                # 判断工程是否存在对应的爬虫服务器, 存在则删除
                if project.project_name in spider_service_instance.list_projects():
                    if not spider_service_instance.delete_project(project.project_name):
                        del_spider_flag = False
                        break
            # 当所有的从服务器的爬虫都删除成功后,删除主服务器的爬虫
            if del_spider_flag:
                for spider_service_instance in self.spider_service_instances_master:
                    if project.project_name in spider_service_instance.list_projects():
                        return spider_service_instance.delete_project(project.project_name)
                    return False

    def get_spider_list(self, project):
        """
        功能: 获取指定工程下的爬虫列表
        :param project: project对象
        :return: 主爬虫的spider对象
        """
        spider_instance_list_slave = []
        spider_instance_list_master = []
        # 获取主爬虫服务器scrapyd上工程名对应的所有spider实例对象
        for spider_service_instance in self.spider_service_instances_master:
            if project.project_name in spider_service_instance.list_projects():
                spider_instance_list_master = \
                    spider_service_instance.get_spider_list(project.project_name)
                break
        # 获取从爬虫服务器scrapyd上工程名对应的所有spider实例对象
        for spider_service_instance in self.spider_service_instances_slave:
            # 如果工程在某个scrapyd服务器上
            if project.project_name in spider_service_instance.list_projects():
                # 取出该服务器上某个工程名下的所有从爬虫
                spider_instance_list_slave = \
                    spider_service_instance.get_slave_spider_list(project.project_name)
                break
        # 判断从爬虫服务器有该工程的爬虫
        if spider_instance_list_slave:
            for spider_instance, slave_spider_name in zip(spider_instance_list_master, spider_instance_list_slave):
                # 给每个spider_instance的project.id赋值
                spider_instance.project_id = project.id
                spider_instance.spider_name_slave = slave_spider_name
            return spider_instance_list_master
        # 判断从爬虫服务器没有该工程的爬虫
        else:
            for spider_instance in spider_instance_list_master:
                # 给每个spider_instance的project.id赋值
                spider_instance.project_id = project.id
            return spider_instance_list_master

    def get_daemon_status(self):
        pass

    def sync_job_status(self, project):
        """
        同步scrapyd服务器上的job状态 到 系统的job_execution任务执行数据库中来
        :param project:
        :return:
        """

        for spider_service_instance in self.spider_service_instances_slave:
            # 从scrapyd中根据爬虫项目名获取爬虫项目下的蜘蛛任务运行状态
            # ex: {'pending': [], 'running': [], 'finish': []}
            job_status = spider_service_instance.get_job_list(project.project_name)
            # 从数据库中获取未完成('pending', 'running')的蜘蛛任务
            job_execution_list = JobExecution.list_uncomplete_job()
            # 根据job_execution 任务执行 数据库中的数据构造 {'任务执行id': '任务执行详情'} 字典
            job_execution_dict = dict(
                [(job_execution.service_job_execution_id.split('>')[-1], job_execution) for job_execution in job_execution_list])
            '''
            把数据库中的job_execution任务执行情况 与 scrapyd中的任务执行情况做匹配
            更新其相应的字段
            '''
            # 正在运行的(PENDING)
            for job_execution_info in job_status[SpiderStatus.RUNNING]:
                job_execution = job_execution_dict.get(job_execution_info['id'])
                if job_execution and job_execution.running_status == SpiderStatus.PENDING:
                    job_execution.start_time = job_execution_info['start_time']
                    job_execution.running_status = SpiderStatus.RUNNING

            # 运行完成的(FINISH)
            for job_execution_info in job_status[SpiderStatus.FINISHED]:
                job_execution = job_execution_dict.get(job_execution_info['id'])
                if job_execution and job_execution.running_status != SpiderStatus.FINISHED:
                    job_execution.start_time = job_execution_info['start_time']
                    job_execution.end_time = job_execution_info['end_time']
                    job_execution.running_status = SpiderStatus.FINISHED
            db.session.commit()

    def start_spider(self, job_instance):
        """
        功能: 启动爬虫,首先启动从爬虫, 至少有一个从爬虫启动成功后启动主爬虫
        :param job_instance: job_instance对象
        :return: None
        """
        project = Project.find_project_by_id(job_instance.project_id)
        if project.is_msd == '0':  # 如果是单机爬虫
            spider_name = job_instance.spider_name
            for leader in self.spider_service_instances_master:
                serviec_job_id = leader.start_spider(project.project_name, spider_name)
                # 如果启动成功
                if serviec_job_id:
                    job_execution = JobExecution()
                    job_execution.project_id = job_instance.project_id
                    job_execution.service_job_execution_id = serviec_job_id + '>'
                    job_execution.job_instance_id = job_instance.id
                    job_execution.create_time = datetime.datetime.now()
                    job_execution.running_on = leader.server + '>'
                    db.session.add(job_execution)
                    db.session.commit()
                    break
        else:
            # 主爬虫名
            spider_name_master = job_instance.spider_name
            spider_instance = SpiderInstance.query.filter_by(
                project_id=job_instance.project_id, spider_name=spider_name_master).first()
            # 从爬虫名
            spider_name_slave = spider_instance.spider_name_slave
            # 启动从爬虫服务器启动成功标志
            slave_flag = False
            # 从爬虫的job执行列表
            serviec_job_id_slave = []
            # 从爬虫运行的服务器列表
            running_on_slave = []
            # 遍历从爬虫服务器
            for leader in self.spider_service_instances_slave:
                # 启动爬虫, 爬虫启动成功,返回id, 否则返回None
                serviec_job_id = leader.start_spider(project.project_name, spider_name_slave)
                # 如果启动成功
                if serviec_job_id:
                    # 标志为True
                    slave_flag = True
                    # job_id添加到列表, 为日志获取提供数据
                    serviec_job_id_slave.append(serviec_job_id)
                    # 运行的服务器添加到列表, 为日志获取提供数据
                    running_on_slave.append(leader.server)
            # 将列表转换为字符串
            serviec_job_id_slave_str = ','.join(serviec_job_id_slave)
            running_on_slave_str = ','.join(running_on_slave)
            # 从爬虫服务器至少有一个启动成功,则启动主爬虫服务器
            if slave_flag:
                for leader in self.spider_service_instances_master:
                    serviec_job_id = leader.start_spider(project.project_name, spider_name_master)
                    # 如果启动成功
                    if serviec_job_id:
                        job_execution = JobExecution()
                        job_execution.project_id = job_instance.project_id
                        job_execution.service_job_execution_id = serviec_job_id+'>'+serviec_job_id_slave_str
                        job_execution.job_instance_id = job_instance.id
                        job_execution.create_time = datetime.datetime.now()
                        job_execution.running_on = leader.server+'>'+running_on_slave_str
                        db.session.add(job_execution)
                        db.session.commit()
                        break

    def cancel_spider(self, job_execution, project_name):
        """
        取消某个项目下所有scrapyd服务器上的执行任务
        :param job_execution: 任务执行记录
        :param project_name: 任务执行的项目名
        """
        # 轮训爬虫代码运行的服务器列表
        for spider_service_instance in self.spider_service_instances_master + self.spider_service_instances_slave:
            if spider_service_instance.server in job_execution.running_on:
                # 切割某项目任务执行历史id, 该值是由所有执行任务服务器上scrapyd的任务id拼接而成
                # 取消所有scrapyd服务器上的执行任务
                for job_id in job_execution.service_job_execution_id.split('>'):
                    if spider_service_instance.cancel_spider(project_name, job_id):
                        job_execution.end_time = datetime.datetime.now()
                        job_execution.running_status = SpiderStatus.CANCELED
                        db.session.commit()


    def deploy(self, project, egg_path_dict, is_msd):
        """
        功能: 将主从爬虫部署到scrapyd上,至少有一个从爬虫部署成功后才部署主爬虫
        :param project: 工程名
        :param egg_path_dict: egg文件路径
        :return: 主爬虫部署成功后返回True, 否则返回False
        """
        if is_msd == '0':  # 如果是单机爬虫从主爬虫代理中选择一个代理部署爬虫代码
            spider_service_instance = random.choice(self.spider_service_instances_master)
            # 部署单机爬虫
            master_flag = spider_service_instance.deploy(
                project.project_name, egg_path_dict.get('egg'))
            # 爬虫部署成功, 则返回True
            if master_flag:
                return True
            else:
                return False
        else:
            slave_flag = False
            # 部署从爬虫
            for spider_service_instance in self.spider_service_instances_slave:
                if spider_service_instance.deploy(
                        project.project_name, egg_path_dict.get("slave")):
                    slave_flag = True
            # 从爬虫至少部署成功一个, 则开始部署主爬虫
            if slave_flag:
                # 遍历主爬虫服务器
                for spider_service_instance in self.spider_service_instances_master:
                        # 部署主爬虫
                        master_flag = spider_service_instance.deploy(
                                        project.project_name, egg_path_dict.get('master'))
                        # 只要有一个主爬虫部署, 则返回True
                        if master_flag:
                            return True
                        else:
                            return False
            else:
                return slave_flag

    def log_url_master(self, job_execution):
        """
        功能: 获取主爬虫的日志
        :param job_execution: job_execution对象
        :return: 返回log的url
        """
        job_instance = JobInstance.find_job_instance_by_id(job_execution.job_instance_id)
        project = Project.find_project_by_id(job_instance.project_id)
        # 主从爬虫运行的服务器字符串
        service_job_execution_id = job_execution.service_job_execution_id.split('>')
        # 主爬虫服务器
        master_service_job_execution_id = service_job_execution_id[0]
        # 爬虫运行的服务器地址
        running_on = job_execution.running_on.split('>')
        master_running_on = running_on[0]
        # 调用主爬虫的日志
        for spider_service_instance in self.spider_service_instances_master:
            if spider_service_instance.server == master_running_on:
                master_log_url = spider_service_instance.log_url(
                    project.project_name, job_instance.spider_name,
                    master_service_job_execution_id)
                return master_log_url

    def log_url_slave(self, job_execution):
        """
        功能: 获取从爬虫的日志,只要获取一个
        :param job_execution: job_execution对象
        :return: 返回log的url
        """
        job_instance = JobInstance.find_job_instance_by_id(job_execution.job_instance_id)
        project = Project.find_project_by_id(job_instance.project_id)
        # 主从爬虫运行的服务器字符串
        service_job_execution_id = job_execution.service_job_execution_id.split('>')
        #  从爬虫服务器列表
        slave_service_job_execution_id = service_job_execution_id[1].split(',')
        # 爬虫运行的服务器地址
        running_on = job_execution.running_on.split('>')
        slave_running_on = running_on[1].split(',')
        # 调用从爬虫的日志
        spider_name_slave_obj = SpiderInstance.query.filter_by(
            spider_name=job_instance.spider_name,
            project_id=job_instance.project_id).first()
        spider_name_slave = spider_name_slave_obj.spider_name_slave

        for spider_service_instance in self.spider_service_instances_slave:
            for job_execution_id, running_on_ in zip(slave_service_job_execution_id, slave_running_on):
                if spider_service_instance.server == running_on_:
                    slave_log_url = spider_service_instance.log_url(
                        project.project_name, spider_name_slave,
                        job_execution_id)
                    return slave_log_url

    @property
    def servers(self):
        return [self.spider_service_instance.server for self.spider_service_instance in
                self.spider_service_instances_master]


if __name__ == '__main__':
    pass
