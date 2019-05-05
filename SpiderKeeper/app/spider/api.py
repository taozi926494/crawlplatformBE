from flask import Blueprint, request
import json
from SpiderKeeper.app.spider.model import JobInstance, Project, JobExecution, SpiderInstance, JobRunType
from SpiderKeeper.app.param_config.model import *
from SpiderKeeper.app import app, agent
from flask import render_template
import requests
from SpiderKeeper.app import db


api_spider_bp = Blueprint('spider', __name__)
# 运行状态选项字典
switcher = {
    0: "PENDING",
    1: "RUNNING",
    2: "FINISHED",
}


@app.route("/", methods=['get'])
def index():
    """
    功能: 用户首页
    :return:
    """
    return render_template('index.html')


@app.route("/allproject", methods=['get'])
def get_project_info():
    """
    功能: 获取工程的相关信息
    :return: 返回数据格式: json, 样式如下：
    {
      "code": 200,
      "data": [
        {
          "applicant": "李科君",
          "create_time": "2018-09-19 03:30:45",
          "developers": ["袁公萍","程培东"],
          "for_project": "全国一体化大数据中心典型应用",
          "project_alias": "深圳市共享开放平台",
          "project_id": 1,
          "project_name": "opendata_sz",
          "is_msd": "0"
          "status": "FINISGED"
        }]}
    """
    data = []  # 临时保存数据列表
    # 遍历数据库中的所有工程
    for project in Project.query.all()[-100:]:
        _dict = project.to_dict()  # 得到工程信息, 字典格式
        # 依据工程的id查询JobExecution表, 获取运行状态信息, 读取最后一个则为最新的信息
        # 判断工程是否是首次运行, 是则读取状态信息, 否则status=PENDING
        _status = JobExecution.query.filter_by(project_id=_dict['project_id']).all()
        if _status:
            _status = _status[-1].running_status
            _dict['status'] = switcher.get(_status, "CANCELED")
        else:
            _dict['status'] = 'PENDING'
        # 将状态信息(数字格式)转换为英文格式
        data.append(_dict)
    return json.dumps({
        'code': 200,
        'data': data
    })


@app.route("/allspider",  methods=['get'])
def get_all_spiders_info():
    """
    功能: 返回所有的爬虫的相关信息
    :return: 返回数据格式: json, 样式如下：
    {
      "last_run_status": "success",
      "last_run_time": "2018-09-19 03:30:45",
      "project_alias": "深圳市共享开放平台",
      "project_id": 1,
      "project_name": "opendata_sz",
      "spider_alias": "所有数据",
      "spider_id": 1,
      "spider_name": "list"
    }
    """
    # 保存数据的临时列表
    data = []
    # 遍历实例数据库, 获取爬虫信息
    job_instance = JobInstance.query.order_by(JobInstance.date_created).group_by(JobInstance.project_id).all()
    for spider in job_instance:
        # 得到实例的字典信息
        _temp = spider.to_dict()
        # 依据工程id查找Project数据库, 获取工程名以及备注信息
        project_base_info = Project.find_project_by_id(_temp['project_id'])
        instance_to_job_execution = JobExecution.query.filter_by(job_instance_id=_temp['job_instance_id']).all()
        if instance_to_job_execution:
            # 获取状态信息
            _status = instance_to_job_execution[-1].running_status
            # 状态信息格式转变
            status = switcher.get(_status, "CANCELED")
            # 获取实例的上一次运行时间
            last_run_time = instance_to_job_execution[-1].end_time
            service_job_execution_id = instance_to_job_execution[-1].service_job_execution_id
        else:
            status = 'PENDING'
            last_run_time = None
            service_job_execution_id = None
        # 将信息封装成字典格式
        _dict = dict(
            project_id=_temp['project_id'],
            project_name=project_base_info.project_name,
            project_alias=project_base_info.project_alias,
            spider_id=SpiderInstance.query.filter_by(project_id=_temp['project_id']).first().id,
            spider_name=_temp['spider_name'],
            spider_alias=_temp['desc'],
            last_run_status=status,
            last_run_time=str(last_run_time).split('.')[0],
            run_type=_temp['run_type'],
            job_exec_id=service_job_execution_id,
            is_msd=project_base_info.is_msd
        )
        data.append(_dict)
    return json.dumps({"code": 200, 'data': data})


@app.route("/projectinfo",  methods=['get'])
def get_single_project_info():
    """
    功能: 返回单个工程的相关信息
    :param project_name: 工程名
    :return: 返回json数据格式, 样例如下：
     {
      "code": 200,
      "data": [{
        "project_id": 1,
        "project_name": "opendata_sz",
        "project_alias": "深圳市共享开放平台",
        "create_time": "2018-09-22 11:30:23",
        "developers": ["程培东","袁公萍"],
        "for_project": "全国一体化大数据中心",
        "applicant": "胥月",
        "spiders": [{
            "spider_id": 1,
            "spider_alias": "所有数据",
            "spider_name": "list",
            "last_run_time": "2018-09-19 03:30:45",
            "last_run_status": "success",
            "circle_type": null
            },{
            "spider_id": 2,
            "spider_alias": "数据",
            "spider_name": "shuju",
            "last_run_time": "2018-09-19 07:37:25",
            "last_run_status": "success",
            "circle_type": "week"
          }
        ]
      }]
    }
    """
    project_name = request.args.get('project_name')
    # 返回的临时spiders数据列表变量
    spiders_info_list = []
    # 依据project_name查询工程信息
    project_info = Project.query.filter_by(project_name=project_name).first()
    # 遍历实例数据库
    for spider in SpiderInstance.query.filter_by(project_id=project_info.id).all():
        # 返回字典信息
        spider_dict = spider.to_dict()
        # 查找该spider对应的实例
        spider_to_job_instances = JobInstance.query.filter_by(
            project_id=spider_dict.get('project_id'),
            spider_name=spider_dict.get('spider_name')).all()
        # 如果spider没有实例
        if not spider_to_job_instances:
            spider_info = dict(
                spider_id=spider_dict.get('spider_instance_id'),
                spider_alias=None,
                spider_name=spider_dict.get('spider_name'),
                last_run_time=None,
                last_run_status=None,
                circle_type=None,
                job_exec_id=None
            )
            spiders_info_list.append(spider_info)
        else:
            for spider_to_job_instance in spider_to_job_instances:
                spider_to_job_instance_dict = spider_to_job_instance.to_dict()
                # 获取实例对象的最后一次运行时间
                spider_job_execution = JobExecution.query.filter_by(
                    job_instance_id=spider_to_job_instance_dict['job_instance_id']).all()
                if len(spider_job_execution) > 0:
                    last_run_time = spider_job_execution[-1].end_time
                    # 获取状态信息
                    _status = spider_job_execution[-1].running_status
                    # 状态信息格式转变
                    status = switcher.get(_status, "CANCELED")
                    job_exec_id = spider_job_execution[-1].service_job_execution_id
                else:
                    status = 'PENDING'
                    last_run_time = None
                    job_exec_id = None

                # 获取每个实例对应的spider的信息
                spider_info = dict(
                    spider_id=spider_dict.get('spider_instance_id'),
                    spider_alias=spider_to_job_instance_dict['desc'],
                    spider_name=spider_dict['spider_name'],
                    last_run_time=str(last_run_time).split('.')[0],
                    last_run_status=status,
                    circle_type=spider_to_job_instance_dict['run_type'],
                    job_exec_id=job_exec_id
                )
                # 将spider信息放入spiders列表里
                spiders_info_list.append(spider_info)
    # 信息合并到工程信息下, 格式字典
    _dict = dict(
        project_id=project_info.id,
        project_name=project_name,
        project_alias=project_info.project_alias,
        create_time=str(project_info.date_created),
        developers=project_info.developers,
        for_project=project_info.for_project,
        applicant=project_info.applicant,
        spiders=spiders_info_list,
        is_msd=project_info.is_msd
    )
    # 数据以列表格式返回
    return json.dumps({"code": 200, 'data': _dict})


@app.route("/history_spider_run_info",  methods=['get'])
def get_history_spider_run_info():
    """
    功能: 获取某个爬虫下的历史运行情况
    :return:
    """
    spider_name = request.args.get('spider_name')
    spider_to_job_instances = JobInstance.query.filter_by(spider_name=spider_name).all()[-100:]
    spiders_info_list = []
    # 如果spider有实例
    if spider_to_job_instances:
        for spider_to_job_instance in spider_to_job_instances:
            spider_to_job_instance_dict = spider_to_job_instance.to_dict()
            # 获取实例对象的的所有运行信息
            spider_job_execution = JobExecution.query.filter_by(
                job_instance_id=spider_to_job_instance_dict['job_instance_id']).all()
            for _spider_job_execution in spider_job_execution:
                _dict = {}
                _dict['start_time'] = _spider_job_execution.start_time
                _dict['end_time'] = _spider_job_execution.end_time
                # 获取状态信息
                _status = _spider_job_execution.running_status
                # 状态信息格式转变
                _dict['status'] = switcher.get(_status, "CANCELED")
                _dict['running_on'] = _spider_job_execution.running_on
                spiders_info_list.append(_dict)
        return json.dumps({"code": 200, 'data': spiders_info_list})
    # 数据以列表格式返回
    return json.dumps({"code": 200, 'data': spiders_info_list})


@app.route("/masterlog",  methods=['get'])
def masterlog():
    project_id = request.args.get('project_id')
    job_exec_id = request.args.get('job_exec_id')
    job_execution = JobExecution.query.filter_by(project_id=project_id, service_job_execution_id=job_exec_id).first()
    res = requests.get(agent.log_url_master(job_execution))
    res.encoding = 'utf8'
    raw = res.text.split('\n')
    if len(raw) > 300:
        raw = raw[:150] + raw[-150:]
        raw = '\n'.join(raw)
    else:
        raw = '\n'.join(raw)
    return json.dumps({"code": 200, 'log': raw.split('\n')})


@app.route("/slavelog",  methods=['get'])
def slavelog():
    project_id = request.args.get('project_id')
    job_exec_id = request.args.get('job_exec_id')
    job_execution = JobExecution.query.filter_by(project_id=project_id, service_job_execution_id=job_exec_id).first()
    res = requests.get(agent.log_url_slave(job_execution))
    res.encoding = 'utf8'
    raw = res.text.split('\n')
    if len(raw) > 300:
        raw = raw[:150] + raw[-150:]
        raw = '\n'.join(raw)
    else:
        raw = '\n'.join(raw)
    return json.dumps({"code": 200, 'log': raw.split('\n')})
