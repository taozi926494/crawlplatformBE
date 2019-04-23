import datetime
from sqlalchemy import desc
from SpiderKeeper.app import db, Base


class Project(Base):
    """
    Project爬虫项目ORM类
    """
    __tablename__ = 'sk_project'
    project_name = db.Column(db.String(50), unique=True)
    applicant = db.Column(db.String(50))  # 申请人
    developers = db.Column(db.String(50))  # 项目的开发者
    for_project = db.Column(db.String(50))  # 提出需求的项目
    project_alias = db.Column(db.String(100))  # 项目的备注
    is_msd = db.Column(db.String(50))  # 是否是主从分布式爬虫 0 单机爬虫 1 分布式爬虫

    @classmethod
    def load_project(cls, project_list):
        """
        将爬虫项目列表里面的爬虫项目添加进入数据库
        :param project_list: 爬虫项目列表
        :return:
        """
        for project in project_list:
            existed_project = cls.query.filter_by(project_name=project.project_name).first()
            if not existed_project:
                db.session.add(project)
                db.session.commit()

    @classmethod
    def find_project_by_id(cls, project_id):
        """
        根据爬虫项目id查找爬虫项目信息
        :param project_id: 爬虫项目id
        :return:
        """
        return Project.query.filter_by(id=project_id).first()

    def to_dict(self):
        return dict(
            project_id=self.id,
            project_name=self.project_name,
            applicant=self.applicant,
            developers=self.developers,
            for_project=self.for_project,
            project_alias=self.project_alias,
            create_time=str(self.date_created),
            is_msd=self.is_msd
        )


class SpiderInstance(Base):
    """
    蜘蛛spider ORM类
    """

    __tablename__ = 'sk_spider'

    spider_name = db.Column(db.String(100))
    project_id = db.Column(db.INTEGER, nullable=False, index=True)
    spider_name_slave = db.Column(db.String(100))

    @classmethod
    def update_spider_instances(cls, project_id, spider_instance_list):
        """
        根据爬虫项目爬虫项目Project id及蜘蛛信息列表, 更新爬虫项目爬虫项目中的Spider蜘蛛信息
        :param project_id: 爬虫项目ID
        :param spider_instance_list: Spider蜘蛛信息列表
        :return:
        """

        # 如果数据库中没有爬虫项目ID及Spider这条记录就往数据库插入该记录
        for spider_instance in spider_instance_list:
            existed_spider_instance = cls.query.filter_by(project_id=project_id,
                                                          spider_name=spider_instance.spider_name).first()
            if not existed_spider_instance:
                db.session.add(spider_instance)
                db.session.commit()

        # 从数据库中取出某个爬虫项目下所有的Spider蜘蛛信息
        # 如果数据库中的Spider蜘蛛不在提交过来的蜘蛛信息列表里面则从数据库中删除该蜘蛛信息
        for spider in cls.query.filter_by(project_id=project_id).all():
            existed_spider = any(
                spider.spider_name == s.spider_name
                for s in spider_instance_list
            )
            if not existed_spider:
                db.session.delete(spider)
                db.session.commit()

    @classmethod
    def list_spider_by_project_id(cls, project_id):
        """
        通过爬虫项目id列出某个爬虫项目在sk_spider表下的所有蜘蛛信息
        :param project_id: 爬虫项目id
        :return: 某个爬虫项目id在sk_spider表下的所有蜘蛛信息
        """
        return cls.query.filter_by(project_id=project_id).all()

    def to_dict(self):
        return dict(spider_instance_id=self.id,
                    spider_name=self.spider_name,
                    spider_name_slave=self.spider_name_slave,
                    project_id=self.project_id)

    @classmethod
    def list_spiders(cls, project_id):
        """
        通过爬虫项目id列出某个爬虫项目下的所有蜘蛛及其任务运行信息(蜘蛛最新的任务的创建时间、平均运行时间)
        :param project_id:  爬虫项目id
        :return: list 某个爬虫项目id下的所有蜘蛛及其任务运行信息
        """

        # 该sql语句用于获取所有蜘蛛最新的任务的创建时间
        # 返回 [(蜘蛛名称1, 最新的任务创建时间), (蜘蛛名称2, 最新的任务创建时间)]
        sql_last_runtime = '''
            select * from (select a.spider_name,b.date_created from sk_job_instance as a
                left join sk_job_execution as b
                on a.id = b.job_instance_id
                order by b.date_created desc) as c
                group by c.spider_name
            '''
        # 该sql语句用于获取所有蜘蛛的平均运行时间
        # 返回 [(蜘蛛名称1, 任务平均运行时间), (蜘蛛名称2, 任务平均运行时间)]
        # ****** 这里有个问题, 实际把sql复制执行的时候, 返回的平均运行时间都是0, 待进一步解决 **********
        sql_avg_runtime = '''
            select a.spider_name,avg(end_time-start_time) from sk_job_instance as a
                left join sk_job_execution as b
                on a.id = b.job_instance_id
                where b.end_time is not null
                group by a.spider_name
            '''
        last_runtime_list = dict(
            (spider_name, last_run_time) for spider_name, last_run_time in db.engine.execute(sql_last_runtime))
        avg_runtime_list = dict(
            (spider_name, avg_run_time) for spider_name, avg_run_time in db.engine.execute(sql_avg_runtime))
        res = []
        for spider in cls.query.filter_by(project_id=project_id).all():
            last_runtime = last_runtime_list.get(spider.spider_name)
            res.append(dict(spider.to_dict(),
                            **{'spider_last_runtime': last_runtime if last_runtime else '-',
                               'spider_avg_runtime': avg_runtime_list.get(spider.spider_name)
                               }))
        return res


class JobPriority():
    LOW, NORMAL, HIGH, HIGHEST = range(-1, 3)


class JobRunType():
    ONETIME = 'onetime'
    PERIODIC = 'periodic'


class JobInstance(Base):
    """
    调度任务ORM类
    """
    __tablename__ = 'sk_job_instance'
    spider_name = db.Column(db.String(100), nullable=False, index=True)  # 蜘蛛名称
    project_id = db.Column(db.INTEGER, nullable=False, index=True)  # 爬虫项目id
    tags = db.Column(db.Text)  # 任务的标签（通过英文逗号隔开）
    spider_arguments = db.Column(db.Text)  # 任务执行参数, 通过英文逗号隔开 (ex.: arg1=foo,arg2=bar)
    priority = db.Column(db.INTEGER)  # 任务优先级
    desc = db.Column(db.Text)  # 任务描述
    cron_minutes = db.Column(db.String(20), default="0")  # 周期调度时间-分钟, 默认是0
    cron_hour = db.Column(db.String(20), default="*")  # 周期调度时间-小时, 默认是*
    cron_day_of_month = db.Column(db.String(20), default="*")  # 周期调度时间-天, 默认是*
    cron_day_of_week = db.Column(db.String(20), default="*")  # 周期调度时间-星期, 默认是*
    cron_month = db.Column(db.String(20), default="*")  # 周期调度时间-月份, 默认是*
    enabled = db.Column(db.INTEGER, default=0)  # 0/-1  # 是否可以被周期调度 0可以 -1不可以
    run_type = db.Column(db.String(20))  # periodic/onetime  调度方式 周期性 和 一次性

    def to_dict(self):
        """
        以字典方式放回Job任务的自身信息
        :return: dict Job任务的自身信息
        """
        return dict(
            job_instance_id=self.id,
            project_id=self.project_id,
            spider_name=self.spider_name,
            tags=self.tags.split(',') if self.tags else None,
            spider_arguments=self.spider_arguments,
            priority=self.priority,
            desc=self.desc,
            cron_minutes=self.cron_minutes,
            cron_hour=self.cron_hour,
            cron_day_of_month=self.cron_day_of_month,
            cron_day_of_week=self.cron_day_of_week,
            cron_month=self.cron_month,
            enabled=self.enabled == 0,
            run_type=self.run_type

        )

    @classmethod
    def list_job_instance_by_project_id(cls, project_id):
        """
        通过爬虫项目id列出其所有的Job任务信息
        :param project_id: 爬虫项目id
        :return: list Job任务信息
        """
        return cls.query.filter_by(project_id=project_id).all()

    @classmethod
    def find_job_instance_by_id(cls, job_instance_id):
        """
        通过Job任务id查询Job任务信息
        :param job_instance_id: Job任务id
        :return: JobInstance Job任务信息
        """
        return cls.query.filter_by(id=job_instance_id).first()


class SpiderStatus():
    PENDING, RUNNING, FINISHED, CANCELED = range(4)


class JobExecution(Base):
    """
    执行任务ORM类
    """
    __tablename__ = 'sk_job_execution'
    project_id = db.Column(db.INTEGER, nullable=False, index=True)  # 爬虫项目id
    service_job_execution_id = db.Column(db.String(50), nullable=False, index=True)  # 任务执行历史id
    job_instance_id = db.Column(db.INTEGER, nullable=False, index=True)  # 对应的执行的调度任务id
    create_time = db.Column(db.DATETIME)  # 该条历史任务的创建时间
    start_time = db.Column(db.DATETIME)  # 执行任务开始时间
    end_time = db.Column(db.DATETIME)  # 执行任务结束时间
    running_status = db.Column(db.INTEGER, default=SpiderStatus.PENDING)  # 执行状态
    running_on = db.Column(db.Text)  # 执行主机 'localhost:6800'

    def to_dict(self):
        """
        以字典方式放回Job任务的自身信息
        :return: dict Job任务的自身信息
        """
        job_instance = JobInstance.query.filter_by(id=self.job_instance_id).first()
        return {
            'project_id': self.project_id,
            'job_execution_id': self.id,
            'job_instance_id': self.job_instance_id,
            'service_job_execution_id': self.service_job_execution_id,
            'create_time': self.create_time.strftime('%Y-%m-%d %H:%M:%S') if self.create_time else None,
            'start_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time else None,
            'end_time': self.end_time.strftime('%Y-%m-%d %H:%M:%S') if self.end_time else None,
            'running_status': self.running_status,
            'running_on': self.running_on,
            'job_instance': job_instance.to_dict() if job_instance else {}
        }

    @classmethod
    def find_job_by_service_id(cls, service_job_execution_id):
        return cls.query.filter_by(service_job_execution_id=service_job_execution_id).first()

    @classmethod
    def list_job_by_service_ids(cls, service_job_execution_ids):
        return cls.query.filter(cls.service_job_execution_id.in_(service_job_execution_ids)).all()

    @classmethod
    def list_uncomplete_job(cls):
        return cls.query.filter(cls.running_status != SpiderStatus.FINISHED,
                                cls.running_status != SpiderStatus.CANCELED).all()

    @classmethod
    def list_jobs(cls, project_id, each_status_limit=100):
        """
        通过爬虫项目id列出前n条 等待执行、正在执行、执行完成的任务信息
        :param project_id: 工程id
        :param each_status_limit: 每个执行状态返回的任务条数, 默认为100条
        :return: dict 每个执行状态的任务信息
        """
        result={}
        result['PENDING'] = [job_execution.to_dict() for job_execution in
                             JobExecution.query.filter_by(project_id=project_id,
                                                          running_status=SpiderStatus.PENDING).order_by(
                                 desc(JobExecution.date_modified)).limit(each_status_limit)]
        result['RUNNING'] = [job_execution.to_dict() for job_execution in
                             JobExecution.query.filter_by(project_id=project_id,
                                                          running_status=SpiderStatus.RUNNING).order_by(
                                 desc(JobExecution.date_modified)).limit(each_status_limit)]
        result['COMPLETED'] = [job_execution.to_dict() for job_execution in
                               JobExecution.query.filter(JobExecution.project_id == project_id).filter(
                                   (JobExecution.running_status == SpiderStatus.FINISHED) | (
                                       JobExecution.running_status == SpiderStatus.CANCELED)).order_by(
                                   desc(JobExecution.date_modified)).limit(each_status_limit)]
        return result

    @classmethod
    def list_run_stats_by_hours(cls, project_id):
        """
        列出一个工程在24小时内每个小时的蜘蛛运行状态, 用于前端可视化展现
        :param project_id: 工程id
        :return: list 每个小时内的运行状态列表 ex: [{'00:00': 6, '01:00': 3}]
        """
        result = {}
        hour_keys = []
        last_time = datetime.datetime.now() - datetime.timedelta(hours=23)
        last_time = datetime.datetime(last_time.year, last_time.month, last_time.day, last_time.hour)
        for hour in range(23, -1, -1):
            time_tmp = datetime.datetime.now() - datetime.timedelta(hours=hour)
            hour_key = time_tmp.strftime('%Y-%m-%d %H:00:00')
            hour_keys.append(hour_key)
            result[hour_key] = 0  # init
        for job_execution in JobExecution.query.filter(JobExecution.project_id == project_id,
                                                       JobExecution.date_created >= last_time).all():
            hour_key = job_execution.create_time.strftime('%Y-%m-%d %H:00:00')
            result[hour_key] += 1
        return [dict(key=hour_key, value=result[hour_key]) for hour_key in hour_keys]
