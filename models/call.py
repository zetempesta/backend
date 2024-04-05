from db.pg import pg
from typing import List
from schema.call import Call
from db.db_conf import db_conf




def newCall(id_user:int, id_research:int, id_contact:int):
    conf = db_conf()
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
    sql = f"""INSERT INTO "public"."calls" ( "id_user", "id_research", "id_contact") 
            VALUES ( {id_user}, {id_research}, {id_contact} )"""
    
    db.executa_sql(sql)
