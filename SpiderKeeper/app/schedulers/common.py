import time
# agent是一个SpiderAgent的实例化对象
# scheduler是Apscheduler的实例化对象
from SpiderKeeper.app import scheduler, app, agent
from SpiderKeeper.app.spider.model import Project, JobInstance, SpiderInstance


def sync_job_execution_status_job():
    """
    sync job execution running status,
    :return:
    """
    for project in Project.query.all():
        agent.sync_job_status(project)
    app.logger.debug('[同步scrapyd上的执行任务到系统数据库]')


def sync_spiders():
    """
    每隔10s同步scrapyd上的爬虫到数据库
    sync spiders
    :return:
    """
    # 遍历所有的工程
    for project in Project.query.all():
        # 通过工程名获取scrapyd上的爬虫列表
        spider_instance_list = agent.get_spider_list(project)
        SpiderInstance.update_spider_instances(project.id, spider_instance_list)
    app.logger.debug('[同步scrapyd上的蜘蛛到系统数据库]')


def run_spider_job(job_instance_id):
    """
    功能: 通过scrapyd启动一个爬虫
    :param job_instance:
    :return:
    """
    try:
        job_instance = JobInstance.find_job_instance_by_id(job_instance_id)
        agent.start_spider(job_instance)
        app.logger.info('[APScheduler调度器调度了一个爬虫任务] [是工程名为: %s] [下的 %s 蜘蛛]'
                        ' [调度任务id为: %s]'
                        % (job_instance.project_id, job_instance.spider_name, job_instance.id))
    except Exception as e:
        app.logger.error('[APScheduler调度器运行爬虫任务出错啦!错误信息为] ' + str(e))


def reload_runnable_spider_job_execution():
    """
    add periodic job to scheduler
    :return:
    """
    running_job_ids = set([job.id for job in scheduler.get_jobs()])  # 从APScheduler中获取当前正在运行的job
    app.logger.debug('[当前正在运行的任务id有: ] %s' % ','.join(running_job_ids))
    # 可以调度的job_id集合
    available_job_ids = set()
    # add new job to schedule
    # 从数据库里面取出所有可以被调度的任务
    for job_instance in JobInstance.query.filter_by(enabled=0, run_type="periodic").all():
        # 构造job_id字符串 spider_job_1:180230238
        job_id = "spider_job_%s:%s" % (job_instance.id, int(time.mktime(job_instance.date_modified.timetuple())))
        # 插入到可以调度的job_id集合里面
        available_job_ids.add(job_id)
        # 如果job_id不在APScheduler现在调度的job里面, 添加该调度任务
        if job_id not in running_job_ids:
            scheduler.add_job(run_spider_job,
                              args=(job_instance.id,),
                              trigger='cron',
                              id=job_id,
                              minute=job_instance.cron_minutes,
                              hour=job_instance.cron_hour,
                              day=job_instance.cron_day_of_month,
                              day_of_week=job_instance.cron_day_of_week,
                              month=job_instance.cron_month,
                              second=0,
                              max_instances=999,
                              misfire_grace_time=60 * 60,
                              coalesce=True)
            '''
            关于scheduler参数的含义
            一个job可能由于某些情况错过执行时间, 比如上一点提到的, 或者是线程池或进程池用光了,
            或者是当要调度job时, 突然down机了等
            这时可以通过设置job的misfire_grace_time选项来指示之后尝试执行的次数
            当然如果这不符合你的期望, 你可以合并所有错过时间的job到一个job来执行, 通过设定job的coalesce = True
            '''
            app.logger.info('[APScheduler调度器中装载了一个爬虫蜘蛛] [是项目名称为: %s 的] [%s 蜘蛛]'
                            ' [这条任务在数据库中的任务id为: %s] [调度器中的job_id为:%s]'
                            % (job_instance.project_id, job_instance.spider_name, job_instance.id, job_id))

    # 删除无效的job_id
    for invalid_job_id in filter(lambda job_id: job_id.startswith("spider_job_"),
                                 running_job_ids.difference(available_job_ids)):
        scheduler.remove_job(invalid_job_id)
        app.logger.info('[从调度器中删除掉了一个调度任务] [任务id为: %s]' % invalid_job_id)


# def reload_runnable_spider_job_execution():
#         """
#         功能: 遍历jobInstance
#         add periodic job to scheduler
#         :return:
#         """
#         # 从数据库里面取出所有可以被调度的任务
#         for job_instance in JobInstance.query.filter_by(enabled=0, run_type="periodic").all():
#             # 构造job_id字符串 spider_job_1:180230238
#             job_id = "spider_job_%s:%s" % (job_instance.id, int(time.mktime(job_instance.date_modified.timetuple())))
#             # 获取当前时间戳
#             current_time = time.time()
#             # 获取当前实例的开始运行时间
#             job_instance_start_run_time = job_instance.start_run_time
#             # 判断是否运行该实例, 如果当前时间大于实例的开始运行时间, 则该爬虫实例
#             if current_time >= job_instance_start_run_time:
#                 run_spider_job(job_instance.id)
#                 # 更新数据库里的start_time
#                 job_instance.start_run_time += job_instance.stride
#                 db.session.commit()

