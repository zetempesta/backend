from db.pg import pg
from schema.answer import db_answer as schema_db_answer
from db.db_conf import db_conf
from typing import List
from datetime import datetime

conf = db_conf()
db = pg(conf.host, conf.database, conf.user, conf.port, conf.password)
date_format = '%Y-%m-%d %H:%M:%S.%f'

def set_contact_tags(idResearch:int):
    sql = """   Select
                    analysis.answer.contato,
                    tag_question.tag As key,
                    analysis.answer.answer As value
                From
                    tag_question Inner Join
                    analysis.answer On analysis.answer.research = tag_question.id_research
                            And analysis.answer.question = tag_question.id_question Inner Join
                    vw_contact_answered On vw_contact_answered.research = analysis.answer.research
                            And vw_contact_answered.contato = analysis.answer.contato
                Order By
                    analysis.answer.research Desc"""
    
    contact_tags = db.consultar_db(sql)

    
    

def fix_results(idResearch)->bool:
    sql = """   Select
                    vw_contact_answered.research,
                    vw_contact_answered.contato,
                    question.wording,
                    question.order_question,
                    city.name As cidade,
                    neighborhood.name As bairro,
                    contact.sex,
                    analysis.answer.answer
                From
                    analysis.answer Inner Join
                    vw_contact_answered On vw_contact_answered.research = analysis.answer.research
                            And vw_contact_answered.contato = analysis.answer.contato Inner Join
                    contact On vw_contact_answered.contato = contact.id Left Join
                    city On city.id = contact.city Left Join
                    neighborhood On contact.neighborhood = neighborhood.id Inner Join
                    question On analysis.answer.question = question.id"""
    db.executa_sql('truncate table "analysis".results')
    answers_list = db.consultar_db(sql)

    # for a in answers_list:


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
