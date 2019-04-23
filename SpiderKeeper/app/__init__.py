# Import flask and template operators
import logging
import traceback
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from flask import jsonify
from flask_basicauth import BasicAuth
from flask_sqlalchemy import SQLAlchemy
from werkzeug.exceptions import HTTPException
from flask_cors import CORS
from SpiderKeeper import config
from flask_login import LoginManager
import datetime

app = Flask(__name__)
# Configurations
app.config.from_object(config)
# 允许跨域请求
CORS(app, resources=r'/*')
cors = CORS(app, supports_credentials=True)
login_manager = LoginManager()  # 初始化flask_login
login_manager.session_protection = 'strong'  # 设置登录安全级别
login_manager.login_view = 'login'  # 指定了未登录时跳转的页面
login_manager.init_app(app)

# Logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
app.logger.setLevel(app.config.get('LOG_LEVEL', "INFO"))
app.logger.addHandler(handler)


db = SQLAlchemy(app, session_options=dict(autocommit=False, autoflush=True))


@app.teardown_request
def teardown_request(exception):
    if exception:
        db.session.rollback()
        db.session.remove()
    db.session.remove()

# Define apscheduler
# 定义apscheduler的后台调度进程
scheduler = BackgroundScheduler()

'''
数据库的Base基类
定义了一些基础字段
'''


class Base(db.Model):
    __abstract__ = True

    id = db.Column(db.Integer, primary_key=True)
    date_created = db.Column(db.DateTime, default=db.func.current_timestamp())
    date_modified = db.Column(db.DateTime, default=db.func.current_timestamp(),
                              onupdate=db.func.current_timestamp())


@app.errorhandler(Exception)
def handle_error(e):
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    app.logger.error(traceback.print_exc())
    return jsonify({
        'code': code,
        'success': False,
        'msg': str(e),
        'data': None
    })

# 创建数据库
from SpiderKeeper.app.spider.model import *


def init_database():
    db.init_app(app)
    db.create_all()


# regist spider service proxy
# SpiderProxy 单个爬虫服务类, 继承单个爬虫服务基类SpiderServiceProxy, 实现基类的功能
# SpiderAgent 爬虫代理服务类, 其实也就是把多个爬虫服务代理的实例统一做一遍轮询操作
from SpiderKeeper.app.proxy.spiderctrl import SpiderAgent
from SpiderKeeper.app.proxy.contrib.scrapy import ScrapydProxy
from SpiderKeeper.app.machine.model import Serversmachine

agent = SpiderAgent()  # 实例化一个蜘蛛


def regist_server():
    # 从数据库中获取主爬虫的服务器并进行注册
    machines = Serversmachine.query.all()
    for machine in machines:
        machine_dict = machine.to_dict()
        agent.regist(ScrapydProxy(machine_dict['server_ip']), machine_dict.get("is_master"))


# ----------------- 注册各个模块的蓝本 -----------------#
from SpiderKeeper.app.spider.controller import ctrl_spider_bp
from SpiderKeeper.app.user.api import api_user_bp
from SpiderKeeper.app.spider.api import api_spider_bp
from SpiderKeeper.app.schedulers.api import api_schedulers_bp
from SpiderKeeper.app.machine.api import api_machine_bp

app.register_blueprint(api_spider_bp)
app.register_blueprint(api_user_bp)
app.register_blueprint(api_schedulers_bp)
app.register_blueprint(ctrl_spider_bp)
app.register_blueprint(api_machine_bp)

# ----------------- 开启异步任务状态调度 -----------------#
from SpiderKeeper.app.schedulers.common import sync_job_execution_status_job, sync_spiders, \
    reload_runnable_spider_job_execution

# 每5秒中就同步scrapyd服务器上的job状态 到 系统的job_execution任务执行数据库中来
scheduler.add_job(sync_job_execution_status_job, 'interval', seconds=5, id='sys_sync_status')
# 每10秒钟就同步scrapyd服务器上的spider 到系统的 spider数据库中来
scheduler.add_job(sync_spiders, 'interval', seconds=10, id='sys_sync_spiders')
# 每隔30秒中就从数据库中找出需要调度的任务进行调度
scheduler.add_job(reload_runnable_spider_job_execution, 'interval', seconds=30, id='sys_reload_job')


def start_scheduler():
    '''
    开启调度器
    :return:
    '''
    scheduler.start()


def init_basic_auth():
    '''
    初始化基础的user权限
    :return:
    '''
    if not app.config.get('NO_AUTH'):
        basic_auth = BasicAuth(app)


def initialize():
    '''
    app初始化, 初始化各个功能模块
    :return:
    '''
    init_database()
    regist_server()
    start_scheduler()
