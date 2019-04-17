from flask_login import UserMixin
from SpiderKeeper.app import db, Base, app
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from itsdangerous import SignatureExpired, BadSignature
from werkzeug.security import generate_password_hash, check_password_hash


class Users(UserMixin,Base):
    __tablename__ = 'sk_users'
    username = db.Column(db.String(64), unique = True)
    password = db.Column(db.String(128))
    email = db.Column(db.String(50))
    roles = db.Column(db.String(128))

    def generate_auth_token(self, expiration=60000):
        '''
        功能：自动生成Token
        :param expiration: token有效时间
        :return: 返回token, 一个加密的字符串
        '''
        s = Serializer(secret_key=app.config['SECRET_KEY'], expires_in=expiration)  # 能够将token的头部和秘钥加密
        return s.dumps(
            {'username': self.username,
             'roles': self.roles})  # 加入载荷进行token加密

    @staticmethod
    def verify_auth_token(token):
        '''
        功能：token认证
        :param token: 自动生成的token
        :return: 完成认证的用户信息
        '''
        s = Serializer(secret_key=app.config['SECRET_KEY'])  # 传入token加密时的秘钥
        try:
            data = s.loads(token)  # token解密
        except SignatureExpired:
            return None  # 认证token是否在有效时间内
        except BadSignature:
            return None  # 认证token是否在有效
        user = Users.query.filter_by(username=data['username']).first()
        return user

    def generate_password(self,password):
        '''
        功能：密码加密
        :param password: 前端获取的密码
        :return: 返回加密的密码
        '''
        return generate_password_hash(password)

    def confirm_password(self,password):
        '''
        功能：加密密码验证
        :param password: 前端获取的密码
        :return: 验证通过返回True, 否则Flase
        '''
        return check_password_hash(self.password, password)



