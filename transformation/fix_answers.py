from db.pg import pg
from schema.answer import db_answer as schema_db_answer
from db.db_conf import db_conf
from typing import List
from datetime import datetime
import pandas as pd
import numpy as np




conf = db_conf()
db = pg(conf.host, conf.database, conf.user, conf.port, conf.password)
date_format = '%Y-%m-%d %H:%M:%S.%f'


def set_contact_tags(idResearch:int):
    sql = """   Select
                    analysis.answer.contato,
                    contact_tag_question.tag As key,
                    analysis.answer.answer As value,
                    analysis.answer.answer As value_update
                From
                    contact_tag_question Inner Join
                    analysis.answer On analysis.answer.research = contact_tag_question.id_research
                            And analysis.answer.question = contact_tag_question.id_question Inner Join
                    vw_contact_answered On vw_contact_answered.research = analysis.answer.research
                            And vw_contact_answered.contato = analysis.answer.contato
                Where
                    analysis.answer.research =  """ + str(idResearch) 
    contact_tags = db.consultar_db(sql)

    list_commands=list()

    for c in contact_tags:

        sql = """   INSERT INTO 
                        "public"."contact_tag" ( "id_contact", "key", "value") VALUES ( {}, '{}', '{}')
                    ON CONFLICT ON CONSTRAINT contact_tag_pkey
                    DO UPDATE SET value = '{}'""".format(c[0],c[1],c[2],c[3])
        list_commands.append(sql)
    db.executa_sql(";".join(list_commands))

def set_research_tags(idResearch:int):
    sql = """ """
    
    contact_tags = db.consultar_db(sql)

    for c in contact_tags:
        sql = """""".format(c[0],c[1], c[2])
        db.executa_sql(sql)        

def fix_data(idResearch)->bool:
    fix_answers()
    set_contact_tags(idResearch)
    
    return True



def fix_answers() -> bool:

    db.executa_sql('truncate table "analysis".answer')
    answers = get_answers()

    fromtoList = db.consultar_db(
        'select FromValue, ToValue from "analysis".fromto')
    fromtoDict = dict()

    for f in fromtoList:
        fromtoDict[f[0]] = f[1]

    fixed_answers = list()

    sql = """INSERT INTO "analysis"."answer" ( "research", "question", "date_time_start", "date_time_end", "contato", "answer") VALUES """
    values = list()

    for answer in answers:

        if answer.answer == '':
            answer.answer = 'Não sabe ou não respondeu'
        else:
            toValue = fromtoDict.get(answer.answer)

            if toValue == None:
                db.executa_sql(
                    "insert into analysis.fromto_dont_founded  values ('" + answer.answer + "')")
            else:
                answer.answer = toValue

        answer.answer = answer.answer.strip()
        format_sql = "({},{},'{}','{}',{},'{}')".format(answer.research, answer.question,
                                                        answer.date_time_start, answer.date_time_end, answer.contato, answer.answer)
    
        values.append(format_sql)

    db.executa_sql(sql + ",".join(values))

    return True


def get_answers() -> List[schema_db_answer]:

    answers = db.consultar_db(
        'SELECT research, question, date_time_start, date_time_end, contato, answer FROM "public".answer')

    return_value = list()

    for a in answers:
        db_answer = schema_db_answer(
            research=a[0], question=a[1], date_time_start=a[2], date_time_end=a[3], contato=a[4], answer=a[5])
        return_value.append(db_answer)

    return return_value
