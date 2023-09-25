from db.pg import pg
from typing import List
from db.db_conf import db_conf
from schema.user import user as user_schema

conf = db_conf()
db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)


def get_user(user, password)->user_schema:
    sql ="""SELECT id,mail,name,username 
            FROM users 
            where   (username = '"""+ user + """' or mail = '"""+ user + """') 
                    and password = '"""+ password +"""'"""
    query = db.consultar_db(sql)
    id = query[0][0]
    mail = query[0][1]
    name = query[0][2]
    username = query[0][3]
    return user_schema(id=id, mail=mail, name=name, username=username)
    
