from flask import Blueprint, flash, request, jsonify, abort
from SpiderKeeper.app import login_manager, app,db
from .model import Users
from flask_login import login_user, logout_user, login_required
import json
from flask import make_response

api_user_bp = Blueprint('user', __name__)


token = ''
@login_manager.user_loader
def load_user(id):
    '''
    功能：实现一个load_user()回调方法, 这个回调用于从会话中存储的用户id重新加载用户对象
    :param id: 用户id
    :return:  id存在则返回对应的用户对象, 不存在则返回none
    '''
    return Users.query.filter_by(id=id).first()


@app.route('/regist', methods=["POST"])
def regist():
    '''
    功能：获取用户注册信息写入数据库
    :return: 返回用户注册信息, json格式如下:
   {
    "code": 200,
    "data":
    {
      "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
      "name": "谢红韬",
      "roles": ["developer", "leader"],
      "token": "eyJhbGciOiJIUzI1NiIsImV4cCI6MTUzODExNDA2NSwiaWF0IjoxNTM4MTE0MDA1fQ.eyJ1c2VyX3JvbGUiOiJkZXZlbG9wZ
      XIsbGVhZGVyIiwidXNlcl9pZCI6Ilx1ODg4MVx1NTE2Y1x1ODQwZCJ9.WFI06CbMb6702KJZrKsNnzI00FqY9cs0XoLUwPAtp1c"
    }}
    '''
    data = request.form  # 获取request表单
    if data is None:
        abort(400)  # 注册信息为空
    if Users.query.filter_by(username=data['username']).first() is not None:
        return jsonify({
            "code": 200,
            "data": {
                "message": "existed"
            }
        })  # 用户已存在
    user = Users()  # 初始化user表
    user.username = data['username']  # 用户名
    user.password = user.generate_password(data['password'])  # 密码
    user.email = data['email']  # 邮箱
    user.roles = 'admin'  # 权限
    db.session.add(user)
    db.session.commit()  # 添加用户信息到数据库
    token = user.generate_auth_token()
    return jsonify({
        "code": 200,
        "data": {
            "roles": user.roles.split(","),
            "token": token.decode('ascii'),
            "name": user.username,
            "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif"
        }
    })


# 用户登录
@app.route('/login', methods=["POST"])
def login():
    '''
    功能：获取用户登录信息
    :return: 返回数据, json格式如下:
   {
    "code": 200,
    "data":
    {
      "token": "eyJhbGciOiJIUzI1NiIsImV4cCI6MTUzODExNDA2NSwiaWF0IjoxNTM4MTE0MDA1fQ.eyJ1c2VyX3JvbGUiOiJkZXZlbG9wZX
      IsbGVhZGVyIiwidXNlcl9pZCI6Ilx1ODg4MVx1NTE2Y1x1ODQwZCJ9.WFI06CbMb6702KJZrKsNnzI00FqY9cs0XoLUwPAtp1c"
    }}
    '''
    # form = Login_Form()
    # global token
    data = request.form  # 获取请求的数据
    username = data['username']  # 登录的用户名
    password = data['password']  # 登录的密码
    user = Users.query.filter_by(username=username).first()  # 从数据库中查询请求的用户信息
    if not user:
        return jsonify({
            "code": 500,
            "message": '无效的用户名'
        })
    elif user.confirm_password(password) is True:  # 验证请求的密码与数据库中的密码是否一致
        login_user(user)  # 用户登录
        token = user.generate_auth_token()
        return jsonify({
            "code": 200,
            "data": {
                "token": token.decode('ascii')
            }
        })
    else:
        return jsonify({
            "code": 500,
            "message": '无效的密码'
        })


# 用户详情信息
@app.route('/userinfo', methods=['GET'])
def userinfo():
    '''
    功能：获取用户登录的具体信息
    :return: 返回数据, json格式如下:
    {
     "code": 200,
     "data":
     {
       "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
       "email": "xiehongtao@cetcbigdata.com",
       "name": "谢红韬",
       "roles": "leader"
     }}
    '''
    token = request.args.get('token')  # 查找登录的用户信息
    user = Users.verify_auth_token(token)
    return jsonify({
            "code": 200,
            "data": {
                "roles":user.roles.split(","),
                "name": user.username,
                "email": user.email,
                "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif"
               }
            })

# 用户登出
@app.route('/logout',methods=["POST","OPTIONS"])
def logout():
    '''
    功能：用户登出
    :return: 返回数据, json格式如下:
    {
     "code": 200,
     "status": "success"
    }
        '''
    # logout_user()  # 用户退出登录
    # flash('退出成功！')
    return jsonify({
         "code": 200,
         "status": "success"
    })

