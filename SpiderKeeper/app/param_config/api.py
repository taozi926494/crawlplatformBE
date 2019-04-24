from SpiderKeeper.app.param_config.model import *
from SpiderKeeper.app.spider.model import *
from flask import Blueprint, request
from SpiderKeeper.app import app, agent, db
import json
from SpiderKeeper.app.proxy.contrib.scrapy import ScrapydProxy


# 注册蓝本
api_machine_bp = Blueprint('param_config', __name__)


@app.route("/addmachine", methods=['POST'])
def addmachine():
    """
    功能:　添加服务器
    :return: 成功返回success, 失败返回相应的异常
    """
    try:
        # 实例化Serversmachine类
        serversmachine = Serversmachine()
        # 给server_ip字段赋值
        serversmachine.server_ip = request.form.get('server_ip')
        # 给is_master字段赋值
        serversmachine.is_master = request.form.get('is_master')
        # 给server_status字段赋值
        serversmachine.server_status = request.form.get('server_status')
        # 保存数据
        db.session.add(serversmachine)
        db.session.commit()
        agent.regist(ScrapydProxy(request.form.get('server_ip')), request.form.get('is_master'))
        return json.dumps({'code': 200, 'data': 'success'})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "添加错误"})


@app.route("/listmachine", methods=['get'])
def listmachine():
    """
    功能: 列出所有的服务器
    :return: 成功返回服务器信息列表,server_ip,is_master,server_status三个字段的信息, data的值格式如下:
             [
             {'server_ip':'http://172.10.10.184:6800',
             'is_master': '0',
             'server_status': '1'}
             ]
             失败则返回空列表
    """
    try:
        machines = Serversmachine.query.all()
        data = []
        for machine in machines:
            machine_dict = machine.to_dict()
            data.append(machine_dict)
        return json.dumps({'code': 200, 'data': data})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "查询错误"})


@app.route("/delmachine", methods=['GET'])
def delmachine():
    """
    功能: 删除服务器的信息
    :return: 成功返回'data': 'success', 失败'data': 'error'
    """
    try:
        serversmachine = Serversmachine()
        server_ip = request.args.get('server_ip')
        machines = serversmachine.query.filter_by(server_ip=server_ip).all()
        if machines:
            for machine in machines:
                db.session.delete(machine)
                db.session.commit()
                machine_dict = machine.to_dict()
                if machine_dict.get('is_master') == '1':
                    for master_machine_instance in agent.spider_service_instances_master:
                        if master_machine_instance._server == server_ip:
                            agent.spider_service_instances_master.remove(master_machine_instance)
                else:
                    for slave_machine_instance in agent.spider_service_instances_slave:
                        if slave_machine_instance._server == server_ip:
                            agent.spider_service_instances_master.remove(slave_machine_instance)
        return json.dumps({'code': 200, 'data': 'success'})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "删除错误"})


@app.route("/adddeveloper", methods=['POST'])
def add_developer():
    """
    功能:　添加开发人员
    :return: 成功返回success, 失败返回相应的异常
    """
    try:
        # 实例化Developer类
        developer = Developer()
        # 给developer_name字段赋值
        developer_name = request.form.get('developer_name')
        developer.developer_name = developer_name
        # 给developer_status字段赋值
        developer.developer_status = request.form.get('developer_status')
        developer.developer_role = request.form.get('developer_role')
        if not developer.developer_name:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员姓名不能为空"})
        if not developer.developer_status:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员状态不能为空"})
        if not developer.developer_role:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员性质不能为空"})
        # 判断工程名是否存在
        existed_developer = developer.query.filter_by(developer_name=developer_name).first()
        # 工程存在则不能保存信息以及部署, 不存在则正常部署
        if existed_developer:
            return json.dumps({"code": 500, "status": 'error', 'msg': '开发人员已存在'})
        # 保存数据
        db.session.add(developer)
        db.session.commit()
        return json.dumps({'code': 200, 'data': 'success'})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "添加错误"})


@app.route("/deldeveloper", methods=['GET'])
def del_developer():
    """
    功能: 删除开发人员的信息
    :return: 成功返回'data': 'success', 失败'data': 'error'
    """
    try:
        developer = Developer()
        developer_name = request.args.get('developer_name')
        if not developer_name:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员姓名不能为空"})
        developers = developer.query.filter_by(developer_name=developer_name).all()
        if developers:
            for person in developers:
                db.session.delete(person)
                db.session.commit()
        return json.dumps({'code': 200, 'data': 'success'})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "删除错误"})


@app.route("/listdeveloper", methods=['get'])
def list_developer():
    """
    功能: 列出所有的开发人员信息
    :return: 成功返回服务器信息列表,developer_name字段的信息, data的值格式如下:
             [
             {'id':1,
             'developer_name':'陈林翠',
             'developer_role':'0'，
             'developer_status':'1'}
             ]
             失败则返回空列表
    """

    try:
        # 先按照开发人员就职状态排序(在职人员在前), 在按照开发人员性质排序(正式员工在前)
        developers = Developer.query.order_by(Developer.developer_status, Developer.developer_role).all()
        data = []
        for developer in developers:
            developer_dict = developer.to_dict()  # 将查询的记录打包成字典返回给前端
            data.append(developer_dict)
        return json.dumps({'code': 200, 'data': data})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "查询错误"})


@app.route("/updatedeveloper", methods=['GET'])
def update_developer():
    """
    功能: 编辑开发人员的信息
    :return: 成功返回'data': 'success', 失败'data': 'error'
    """
    try:
        developer_id = request.args.get('id')
        developer_status = request.args.get('developer_status')
        developer_role = request.args.get('developer_role')
        developer_name = request.args.get('developer_name')
        if not developer_name:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员姓名不能为空"})

        if not developer_status:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员状态不能为空"})

        if not developer_role:
            return json.dumps({"code": 500, "status": "error", "msg": "开发人员性质不能为空"})

        developer = Developer.query.filter_by(id=developer_id).first()
        developer.developer_name = developer_name
        developer.developer_status = developer_status
        developer.developer_role = developer_role
        db.session.commit()
        return json.dumps({'code': 200, 'data': 'success'})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "修改错误"})

