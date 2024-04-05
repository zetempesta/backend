from db.pg import pg
from typing import List
from schema.call import Call

from db.db_conf import db_conf

conf = db_conf()
db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)


def newCall(id_user:int, id_research:int, id_contact:int):
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
