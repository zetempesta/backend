from db.pg import pg
from typing import List
from schema.city import city

from db.db_conf import db_conf

conf = db_conf()
db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)

def get_cities()->List[city]:

    cities = list()
    

    query =  db.consultar_db('Select city.id, city.name From city Order By city.name')
    
    for row in query:
       
        c = city(id=int(row[0]),name=str(row[1]))
        cities.append(c)

    return cities