from db.pg import pg
from typing import List
from schema.neighborhood import neighborhood

from db.db_conf import db_conf

conf = db_conf()
db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)

def get_neighborhood()->List[neighborhood]:

    neighborhoodList = list()
    

    query =  db.consultar_db('Select neighborhood.id, neighborhood.name, city From neighborhood Order By neighborhood.name')
    
    for row in query:
       
        n = neighborhood(id=int(row[0]),name=str(row[1]),city=row[2])
        neighborhoodList.append(n)


    return neighborhoodList