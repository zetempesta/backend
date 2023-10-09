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


def generate_parquet(id_research:int)->bool:
    # TODO Incluir key-valeu do Research
    conf = db_conf()
    db = pg(conf.host, conf.database, conf.user, conf.port, conf.password)

    sql = """Select
        "vca"."research",
        "vca"."contato",
        LPad("qu"."order_question"::Text, 2, '0') || '-' || "qu"."wording" As "wording",
        "aa"."question",
        "qu"."order_question",
        "ci"."name" As "cidade",
        "ne"."name" As "bairro",
        "con"."sex",
        "aa"."answer",
        "re"."name" As "research_name",
        "ne"."region",
        "ct"."key" As "ct_key",
        "ct"."value" As "ct_value"
    From
        "research"."analysis"."answer" "aa" Inner Join
        "research"."public"."vw_contact_answered" "vca" On "vca"."research" = "aa"."research"
                And "vca"."contato" = "aa"."contato" Inner Join
        "research"."public"."contact" "con" On "vca"."contato" = "con"."id" Inner Join
        "research"."public"."city" "ci" On "ci"."id" = "con"."city" Inner Join
        "research"."public"."neighborhood" "ne" On "con"."neighborhood" = "ne"."id" Inner Join
        "research"."public"."question" "qu" On "aa"."question" = "qu"."id" Inner Join
        "research"."public"."research" "re" On "re"."id" = "aa"."research" Left Join
        "research"."public"."contact_tag" "ct" On "ct"."id_contact" = "con"."id" Left Join
        "research"."public"."research_tag" "reta" On "vca"."research" = "reta"."id_research"
                And "vca"."contato" = "reta"."id_contact"
    Where
        "vca"."research" = """ + str(id_research) + """ And
        "ct"."key" Is Not Null And
        "ct"."value" Is Not Null"""

    dat = pd.read_sql_query(sql=sql, con=db.Conn, dtype={'wording': 'str', 'cidade': 'str', 'bairro': 'str','sex': 'str','answer': 'str','research_name': 'str','region': 'str'})

    dat['sex'].replace(['Female', 'female', 'f', 'feminino'],
                    'Feminino', inplace=True)
    dat['sex'].replace(['m','Male', 'male', 'masculino'], 'Masculino', inplace=True)
    dat['sex'].replace(['', None, 'a'], 'A', inplace=True)

    df_pivoted = dat.pivot(index=["research", "contato", "question", "wording", "order_question", "cidade", "bairro", "sex",
                        "answer", "research_name", "region"], columns=["ct_key"], values=["ct_value"])

    df_pivoted.columns = [f'{j}_{i}' for i, j in df_pivoted.columns]

    df = df_pivoted 
    for c in df.columns:
        df = df.rename(columns={c: c.replace('_ct_value','')})


    df.to_csv(path_or_buf='results.csv',sep='|', mode='w', encoding='utf-8', lineterminator='\n')
    df.to_parquet(path='results.parquet')


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
    print('Consultou')
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
    generate_parquet(idResearch)
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
