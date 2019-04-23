from SpiderKeeper.app import Base
from SpiderKeeper.app import db


class Serversmachine(Base):

    __tablename__ = 'sk_serversmachine'
    server_ip = db.Column(db.String(50))   # 服务器的ip
    server_status = db.Column(db.String(50))   # 主从服务器运行状态, 0不可用,1可用
    is_master = db.Column(db.String(50))  # 主从服务器的标志, 0从服务器,1主服务器

    def to_dict(self):
        return dict(
            server_ip=self.server_ip,
            server_status=self.server_status,
            is_master=self.is_master
        )


class Developer(Base):
    __tablename__ = 'sk_developer'
    developer_name = db.Column(db.String(50), unique=True)  # 开发人员名称
    developer_role = db.Column(db.String(50))  # 开发人员性质, 正式员工、实习生、借调人员
    developer_status = db.Column(db.String(50))  # 开发人员状态, 0不在职,1在职

    def to_dict(self):
        return dict(
            id=self.id,
            developer_name=self.developer_name,
            developer_role=self.developer_role,
            developer_status=self.developer_status,
        )

class CiteProject(Base):
    """
    需求
    """
    name = db.Column(db.String(50), unique=True)


class ThemeProject(Base):
    """
    主题
    """
    name = db.Column(db.String(50), unique=True)


class IndustryProject(Base):
    """"
    行业
    """
    name = db.Column(db.String(50), unique=True)


class DeveloperProject(Base):
    name = db.Column(db.String(50), unique=True)


class TagProjectShip(Base):
    """
    工程与标签的关系
    """
    project_name = db.Column(db.String(50), unique=True)
    developer_name = db.Column(db.String(50))
    cite_name = db.Column(db.String(50))
    theme_name = db.Column(db.String(50))
    industry_name = db.Column(db.String(50))

