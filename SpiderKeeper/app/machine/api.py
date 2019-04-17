from SpiderKeeper.app.machine.model import Serversmachine
from flask import Blueprint, request
from SpiderKeeper.app import app, agent, db
import json
from SpiderKeeper.app.proxy.contrib.scrapy import ScrapydProxy


# 注册蓝本
api_machine_bp = Blueprint('machine', __name__)


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


@app.route("/delmachine", methods=['POST'])
def delmachine():
    """
    功能: 删除服务器的信息
    :return: 成功返回'data': 'success', 失败'data': 'error'
    """
    try:
        serversmachine = Serversmachine()
        server_ip = request.form.get('server_ip')
        machines = serversmachine.query.filter_by(server_ip=server_ip).all()
        if machines:
            for machine in machines:
                db.session.delete(machine)
                db.session.commit()
                machine_dict = machine.to_dict()
                if machine_dict.get('is_master') == '1':
                    agent.spider_service_instances_master.remove(machine_dict.get('server_ip'))
                else:
                    agent.spider_service_instances_slave.remove(machine_dict.get('server_ip'))
        return json.dumps({'code': 200, 'data': 'success'})
    except Exception as e:
        return json.dumps({"code": 500, "status": "error", "msg": "删除错误"})
