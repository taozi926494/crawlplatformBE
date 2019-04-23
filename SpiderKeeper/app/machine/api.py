from SpiderKeeper.app.machine.model import *
from SpiderKeeper.app.spider.model import *
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
            return json.dumps({"code": 500, "status": "error", "msg": "获取人员姓名出错"})
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
             'developer_role':'正式员工'，
             'developer_status':'在职'}
             ]
             失败则返回空列表
    """

    try:
        developers = Developer.query.order_by(Developer.developer_status, Developer.developer_role).all()
        data = []
        for developer in developers:
            developer_dict = developer.to_dict()
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


@app.route('/citeadd', methods=['get'])
def citeadd():
    try:
        name = request.args.get('name')
        obj = CiteProject(name=name)
        db.session.add(obj)
        db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "添加成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "添加失败"})


@app.route('/themeadd', methods=['get'])
def themeadd():
    try:
        name = request.args.get('name')
        obj = ThemeProject(name=name)
        db.session.add(obj)
        db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "添加成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "添加失败"})


@app.route('/industryadd', methods=['get'])
def industryadd():
    try:
        name = request.args.get('name')
        obj = IndustryProject(name=name)
        db.session.add(obj)
        db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "添加成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "添加失败"})


@app.route('/citedel', methods=['get'])
def citedel():
    try:
        '''删除项目引用库的某条记录'''
        # 获取参数
        name = request.args.get('name')
        # 先查询到有待删除的记录
        obj = CiteProject.query.filter_by(name=name).first()
        # 删除记录
        db.session.delete(obj)
        # 提交删除
        db.session.commit()

        '''同时删除该条记录对应的所有工程的标签信息中的该条记录'''
        objs = TagProjectShip.query.filter_by(cite_name=name).all()
        for obj in objs:
            obj.cite_name = None
            db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "删除成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "删除失败"})


@app.route('/themedel', methods=['get'])
def themedel():
    try:
        name = request.args.get('name')
        obj = ThemeProject.query.filter_by(name=name).first()
        db.session.delete(obj)
        db.session.commit()

        '''同时删除该条记录对应的所有工程的标签信息中的该条记录'''
        objs = TagProjectShip.query.filter_by(theme_name=name).all()
        for obj in objs:
            obj.theme_name = None
            db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "删除成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "删除失败"})


@app.route('/industrydel', methods=['get'])
def industrydel():
    try:
        name = request.args.get('name')
        obj = IndustryProject.query.filter_by(name=name).first()
        db.session.delete(obj)
        db.session.commit()

        '''同时删除该条记录对应的所有工程的标签信息中的该条记录'''
        objs = TagProjectShip.query.filter_by(industry_name=name).all()
        for obj in objs:
            obj.industry_name = None
            db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "删除成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "删除失败"})


@app.route('/citeupdate', methods=['get'])
def citeupdate():
    try:
        name = request.args.get('name')
        old_name = request.args.get('old_name')
        obj = CiteProject.query.filter_by(name=old_name).first()
        obj.name = name
        db.session.commit()

        '''同时更新该条记录对应的所有工程的标签信息中的该条记录'''
        objs = TagProjectShip.query.filter_by(cite_name=old_name).all()
        for obj in objs:
            obj.cite_name = name
            db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "更新成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "更新失败"})


@app.route('/themeupdate', methods=['get'])
def themeupdate():
    try:
        name = request.args.get('name')
        old_name = request.args.get('old_name')
        obj = ThemeProject.query.filter_by(name=old_name).first()
        obj.name = name
        db.session.commit()

        '''同时更新该条记录对应的所有工程的标签信息中的该条记录'''
        objs = TagProjectShip.query.filter_by(theme_name=old_name).all()
        for obj in objs:
            obj.theme_name = name
            db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "更新成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "更新失败"})


@app.route('/industryupdate', methods=['get'])
def industryupdate():
    try:
        name = request.args.get('name')
        old_name = request.args.get('old_name')
        obj = IndustryProject.query.filter_by(name=old_name).first()
        obj.name = name
        db.session.commit()

        '''同时更新该条记录对应的所有工程的标签信息中的该条记录'''
        objs = TagProjectShip.query.filter_by(industry_name=old_name).all()
        for obj in objs:
            obj.industry_name = name
            db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "更新成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "更新失败"})


@app.route('/editprojectbaseinfo', methods=['post'])
def edit_project_base_info():
    """
    功能：对工程的基础信息进行更新
    传入参数三个列表: 开发人列表、申请人列表、工程标签列表
    :return:
    """
    try:
        project_name_old = request.form.get('project_name_old')  # 项目的英文名
        project_name = request.form.get('project_name')  # 项目的英文名
        project_name_alias = request.form.get('project_name_alias')  # 项目的中文名
        developers_list = request.form.get('developers')  # 项目的开发者
        applicants_list = request.form.get('applicants')  # 项目的申请者
        tags_dict = request.form.get('tags')  # 项目的分类标签

        # 修改工程标的信息
        obj = Project.query.filter_by(name=project_name_old).first()
        obj.name = project_name
        obj.project_alias = project_name_alias
        obj.applicant = ';'.join(applicants_list)
        db.session.commit()

        # 修改关系映射表的信息
        obj = TagProjectShip.query.filter_by(project_name=project_name_old).first()
        obj.project_name = project_name
        obj.cite_name = tags_dict.get('cite_name')
        obj.theme_name = tags_dict.get('theme_name')
        obj.industry_name = tags_dict.get('industry_name')
        obj.developers_name = ';'.join(developers_list)
        db.session.commit()
        return json.dumps({"code": 200, "status": "sucess", "msg": "修改成功"})
    except:
        return json.dumps({"code": 500, "status": "error", "msg": "修改失败"})