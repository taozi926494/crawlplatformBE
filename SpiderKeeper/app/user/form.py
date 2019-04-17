#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author:chenlincui
#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author:chenlincui
"""表单类"""
from wtforms import StringField,SubmitField,PasswordField
from wtforms.validators import DataRequired, ValidationError, EqualTo, Length, Email
from flask_wtf import FlaskForm
from .model import *

#登录表单
class Login_Form(FlaskForm):
    username = StringField('username',validators=[DataRequired(message='no empty')])
    password = PasswordField('pssword',validators=[DataRequired(message='no empty')])
    submit = SubmitField('登录')


#注册表单
class Register_Form(FlaskForm):
    username = StringField('用户名',
                           validators=[DataRequired(message='用户名不能为空'), Length(6, 12, message='用户名只能在6~12个字符之间')])
    password = PasswordField('密码', validators=[DataRequired(message='密码不能为空'), Length(6, 20, message='密码只能在6~20个字符之间')])
    confirm = PasswordField('确认密码', validators=[EqualTo('password', message='两次密码不一致')])
    email = StringField('邮箱', validators=[Email(message='无效的邮箱格式')])
    submit = SubmitField('立即注册')

    def validate_username(self, field):
        if Users.query.filter_by(username=field.data).first():
            raise ValidationError('用户名已注册，请选用其它名称')






