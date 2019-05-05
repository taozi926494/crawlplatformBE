import json
import os
import tempfile
from werkzeug.utils import secure_filename
from flask import Blueprint, request
from SpiderKeeper.app import app, agent, db
from SpiderKeeper.app.spider.model import JobInstance, Project, JobExecution, SpiderInstance, JobRunType
import datetime

# 注册调度蓝本
api_schedulers_bp = Blueprint('schedulers', __name__)


@app.route("/addproject", methods=['post'])
def add_project():
    """
    功能: 创建工程, 上传工程的egg文件, 将工程部署到scrapyd服务器上
    :param: project_name: 工程名称
    :param: project_alias: 工程备注或中文名称
    :param: for_project: 引用工程
    :param: developers: 爬虫项目的开发者
    :param: applicant: 爬虫申请人
    :param: egg_file: 待上传的爬虫项目的egg文件
    :return: 返回数据格式: json, 部署成功,返回success, 否则返回error
    """
    status = 'error'
    project = Project()
    is_msd = request.form.get('is_msd')  # 工程名
    project.is_msd = is_msd
    project.project_name = request.form.get('project_name')  # 工程名
    project.project_alias = request.form.get('project_alias')  # 工程备注
    project.for_project = request.form.get('for_project', None)  # 引用工程
    project.developers = request.form.get('developers', None)  # 开发者
    project.applicant = request.form.get('applicant', None)  # 申请人
    project.date_created = datetime.datetime.now().strptime(
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    project.date_modified = datetime.datetime.now().strptime(
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')


    if not is_msd:
        return json.dumps({"code": 500, "status": '项目类型不能为空, 项目类型为分布式or单机'})
    # 判断是否有值输入
    if request.form.get('project_name') == '' or request.form.get('project_alias') == '':
        return json.dumps({"code": 200, "status": 'no input!'})

    # 判断工程名是否存在
    existed_project = project.query.filter_by(project_name=request.form['project_name']).first()
    # 工程存在则不能保存信息以及部署, 不存在则正常部署
    if existed_project:
        return json.dumps({"code": 200, "status": 'existed'})

    egg_path_dict = {}  # 用于存放egg文件保存路径

    if is_msd == '0':  # 如果为单机爬虫
        egg = request.files['egg']
        if egg:
            filename = secure_filename(egg.filename)  # 获取egg文件名
            dst_egg = os.path.join(tempfile.gettempdir(), filename)  # 拼接文件路径
            egg.save(dst_egg)  # 保存egg文件
            egg_path_dict['egg'] = dst_egg  # 将项目文件路径保存到egg路径字典中
        else:  # 如果有一个没有上传文件
            return json.dumps({"code": 500, "status": ' egg file are must'})
    else:
        # 获取上传文件
        master_egg = request.files['master_egg']
        slave_egg = request.files['slave_egg']
        # 判断表单是否传入文件
        if master_egg and slave_egg:
            master_filename = secure_filename(master_egg.filename)  # 获取master文件名
            slave_filename = secure_filename(slave_egg.filename)  # 获取slave文件名
            dst_master_egg = os.path.join(tempfile.gettempdir(), master_filename)  # 拼接文件路径
            dst_slave_egg = os.path.join(tempfile.gettempdir(), slave_filename)  # 拼接文件路径
            slave_egg.save(dst_slave_egg)  # 保存slave文件
            master_egg.save(dst_master_egg)  # 保存master文件
            egg_path_dict['master'] = dst_master_egg  # 将master项目文件路径保存到egg路径字典中
            egg_path_dict['slave'] = dst_slave_egg  # 将slave项目文件路径保存到egg路径字典中
        else:  # 如果有一个没有上传文件
            return json.dumps({"code": 500, "status": 'master and slave egg are must'})

    if agent.deploy(project, egg_path_dict, is_msd):
        status = 'success'
        # 部署成功后才将数据保存至数据库
        db.session.add(project)
        db.session.commit()
        return json.dumps({"code": 200, "status": status})
    else:
        return json.dumps({"code": 500, "status": "error", "msg": "部署错误"})


@app.route("/delproject", methods=['post'])
def del_project():
    """
    功能: 通过project_id删除工程,首先在scrapyd服务器进行删除,
          然后同步数据进行删除
    :param project_id: 工程id
    :return: 如果在scrapyd服务器删除成功, 且数据库同步后返回success, 否则返回error
    """
    try:
        project_name = request.form.get('project_name')
        # 依据id检索工程
        project = Project.query.filter_by(project_name=project_name).first()
        # 判断scrapyd服务器是否删除成功, 成功则进行数据库同步, 并返回status

        if agent.delete_project(project):
            # 删除工程
            db.session.delete(project)
            # 删除数据裤中对应工程下的spider
            spiders = SpiderInstance.query.filter_by(project_id=project.id).all()
            for spider in spiders:
                db.session.delete(spider)
            # 删除数据裤中对应工程下的job_instances
            instances = JobInstance.query.filter_by(project_id=project.id).all()
            for instance in instances:
                db.session.delete(instance)
            # 删除数据裤中对应工程下的job_execution
            executions = JobExecution.query.filter_by(project_id=project.id).all()
            for execution in executions:
                db.session.delete(execution)
            db.session.commit()
            return json.dumps({"code": 200, "status": "success"})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "删除错误"})


@app.route("/runonce", methods=['post'])
def run_once():
    """
    功能: 单次运行爬虫
    :param: project_id: 工程id
    :param: spider_name: 爬虫名称
    :param: spider_arguments: 爬虫需要传入的参数
    :param: priority: 任务的优先级
    :param: daemon: 任务线程的类型, 是否为守护线程
    :return: json.dumps({"code": 200, "status": "success/e"}), e指具体抛出的异常
    """
    try:
        # 实例化JobInstance表
        job_instance = JobInstance()
        # 获取工程id参数
        project_id = request.form.get('project_id')
        # 获取爬虫名称并保存
        job_instance.spider_name = request.form.get('spider_name')
        # 保存project_id信息
        job_instance.project_id = project_id
        # 保存爬虫的参数信息
        job_instance.spider_arguments = request.form.get('spider_arguments')
        # 获取爬虫任务的优先级参数并保存
        job_instance.priority = request.form.get('priority', 0)
        # 将爬虫运行类型设置一次性运行方式
        job_instance.run_type = 'onetime'
        # 设置进程的类型
        if request.form['daemon'] != 'auto':
            spider_args = []
            if request.form['spider_arguments']:
                spider_args = request.form['spider_arguments'].split(",")
            spider_args.append("daemon={}".format(request.form['daemon']))
            job_instance.spider_arguments = ','.join(spider_args)
        # 设置不可周期调度
        job_instance.enabled = -1
        # 数据库保存信息
        db.session.add(job_instance)
        db.session.commit()
        # 启动爬虫实例
        agent.start_spider(job_instance)
        return json.dumps({"code": 200, "status": "success"})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "运行错误"})


@app.route("/cancelspider", methods=['post'])
def cancel_spider():
    """
    功能: 取消运行爬虫
    :param: project_id: 工程id
    :return: json.dumps({"code": 200, "status": "success/e"}), e指具体抛出的异常
    """

    try:
        # 获取工程id参数
        project_id = request.form.get('project_id')
        project_name = request.form.get('project_name')
        index = int(request.form.get('index'))  # 获取正在执行的爬虫index
        # 同一个项目id可能有多条执行任务记录, 因此需要获取index来判断要取消某个项目id下的第几条执行任务
        job_execution = JobExecution.query.filter_by(project_id=project_id).all()[index]
        agent.cancel_spider(job_execution, project_name)
        return json.dumps({"code": 200, "status": "success"})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "取消失败"})



@app.route("/addscheduler", methods=['post'])
def add_scheduler():
    """
    功能: 给爬虫添加周期调度实例, 添加成功后数据库同步
    :param: project_id: 工程id
    :param: spider_name: 爬虫名称
    :param: spider_arguments: 爬虫需要传入的参数
    :param: priority: 任务的优先级
    :param: daemon: 任务线程的类型, 是否为守护线程
    :param: cron_minutes: 调度周期参数-分钟
    :param: cron_hour: 调度周期参数-小时
    :param: cron_day_of_month: 调度周期参数-每月的天
    :param: cron_day_of_week: 调度周期参数-每周的星期
    :return: json.dumps({"code": 200, "status": "success/e"}), e指具体抛出的异常
    """
    try:
        project_id = request.form.get('project_id')
        job_instance = JobInstance()
        job_instance.spider_name = request.form['spider_name']
        job_instance.project_id = project_id
        job_instance.spider_arguments = request.form['spider_arguments']
        job_instance.priority = request.form.get('priority', 0)
        job_instance.run_type = 'periodic'
        # chose daemon manually
        if request.form['daemon'] != 'auto':
            spider_args = []
            if request.form['spider_arguments']:
                spider_args = request.form['spider_arguments'].split(",")
            spider_args.append("daemon={}".format(request.form['daemon']))
            job_instance.spider_arguments = ','.join(spider_args)

        job_instance.cron_minutes = request.form.get('cron_minutes') or '0'
        job_instance.cron_hour = request.form.get('cron_hour') or '*'
        job_instance.cron_day_of_month = request.form.get('cron_day_of_month') or '*'
        job_instance.cron_day_of_week = request.form.get('cron_day_of_week') or '*'
        job_instance.cron_month = request.form.get('cron_month') or '*'
        if request.form.get('cron_exp'):
            job_instance.cron_minutes, job_instance.cron_hour, job_instance.cron_day_of_month, job_instance.cron_day_of_week, job_instance.cron_month = \
                request.form['cron_exp'].split(' ')
        db.session.add(job_instance)
        db.session.commit()
        return json.dumps({"code": 200, "status": "success"})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "运行错误"})


@app.route("/remove_job", methods=['get'])
def remove_job():
    """
    功能: 删除job_instance
    传入参数: 分别传入工程id与job实例id: job_instance_id
    :return:
    """
    try:
        job_instance_id = request.args.get('job_instance_id')
        job_instance = JobInstance.query.filter_by(id=job_instance_id).first()
        db.session.delete(job_instance)
        db.session.commit()
        return json.dumps({"code": 200, "status": "success"})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "移除错误"})







