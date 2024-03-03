from db.pg import pg
from schema.answer import db_answer as schema_db_answer
from db.db_conf import db_conf
from typing import List
from datetime import datetime
import pandas as pd
import numpy as np
from transformation.fix_answers import set_contact_tags, fix_answers


def results(id_research: int) -> bool:
    # TODO Incluir key-valeu do Research
    conf = db_conf()
    db = pg(conf.host, conf.database, conf.user, conf.port, conf.password)

    sql = 'DELETE FROM "analysis"."results" WHERE "research" = {}'.format(
        id_research)

    db.executa_sql(sql)

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
        "vca"."research" = 0 And
        "ct"."key" Is Not Null And
        "ct"."value" Is Not Null"""

    dat = pd.read_sql_query(sql=sql, con=db.Conn, dtype={
                            'wording': 'str', 'cidade': 'str', 'bairro': 'str', 'sex': 'str', 'answer': 'str', 'research_name': 'str', 'region': 'str'})

    dat['sex'].replace(['Female', 'female', 'f', 'feminino'],
                       'Feminino', inplace=True)
    dat['sex'].replace(['m', 'Male', 'male', 'masculino'],
                       'Masculino', inplace=True)
    dat['sex'].replace(['', None, 'a'], 'A', inplace=True)

    df_pivoted = dat.pivot(index=["research", "contato", "question", "wording", "order_question", "cidade", "bairro", "sex",
                                  "answer", "research_name", "region"], columns=["ct_key"], values=["ct_value"])

    df_pivoted.columns = [f'{j}_{i}' for i, j in df_pivoted.columns]

    df = df_pivoted
    for c in df.columns:
        df = df.rename(columns={c: c.replace('_ct_value', '')})

    df.reset_index(inplace=True)

    for c in df.columns:
        print(c)

    df["answer"].replace(to_replace=['Não ouvi falar','Não Sabe ou Não quer responder'],
                         value='Não sabe ou não respondeu', inplace=True)
    df["Religião"].replace(to_replace='Não sabe ou não respondeu',
                           value='Não tem ou Não quer responder', inplace=True)

    values_row = list()
    sql = 'INSERT INTO "analysis"."results" ( "research", "contato", "wording", "order_question", "cidade", "bairro", "sex", "answer", "research_name", "region", "question", "faixa_etaria", "religiao") VALUES '

    for index, row in df.iterrows():
        value = "({}, {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}')".format(row["research"], row["contato"], row["wording"], row["order_question"],
                                                                                                row["cidade"], row["bairro"], row["sex"], row["answer"], row["research_name"], row["region"], row["question"], row["Faixa etária"], row["Religião"])
        values_row.append(value)

    db.executa_sql(sql + ", ".join(values_row))


def extra_results(id_research: int) -> bool:
    # TODO Incluir key-valeu do Research
    conf = db_conf()
    db = pg(conf.host, conf.database, conf.user, conf.port, conf.password)

    sql = 'DELETE FROM "analysis"."extra_results" WHERE "research" = {}'.format(
        id_research)

    db.executa_sql(sql)

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
        "vca"."research" = 0 And
        "ct"."key" Is Not Null And
        "ct"."value" Is Not Null"""

    dat = pd.read_sql_query(sql=sql, con=db.Conn, dtype={
                            'wording': 'str', 'cidade': 'str', 'bairro': 'str', 'sex': 'str', 'answer': 'str', 'research_name': 'str', 'region': 'str'})

    dat['sex'].replace(['Female', 'female', 'f', 'feminino'],
                       'Feminino', inplace=True)
    dat['sex'].replace(['m', 'Male', 'male', 'masculino'],
                       'Masculino', inplace=True)
    dat['sex'].replace(['', None, 'a'], 'A', inplace=True)

    df_pivoted = dat.pivot(index=["research", "contato", "question", "wording", "order_question", "cidade", "bairro", "sex",
                                  "answer", "research_name", "region"], columns=["ct_key"], values=["ct_value"])

    df_pivoted.columns = [f'{j}_{i}' for i, j in df_pivoted.columns]

    df = df_pivoted
    for c in df.columns:
        df = df.rename(columns={c: c.replace('_ct_value', '')})

    df.reset_index(inplace=True)

    for c in df.columns:
        print(c)

    df["answer"].replace(to_replace=['Não ouvi falar','Não Sabe ou Não quer responder'],
                         value='Não sabe ou não respondeu', inplace=True)
    df["Religião"].replace(to_replace='Não sabe ou não respondeu',
                           value='Não tem ou Não quer responder', inplace=True)
    

    values_row = list()
    sql = """INSERT INTO "analysis"."extra_results" ( "research", "contato", "wording", "order_question", "cidade", "bairro", "sex", "answer", "research_name", 
    "region", "question", "faixa_etaria", "religiao", "votara", "gestao_emanuel", "gestao_mauro", "gestao_lula") VALUES """

    for index, row in df.iterrows():
        value = """({}, {}, '{}', {}, '{}', '{}', '{}', '{}', 
                    '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}')""".format(row["research"], row["contato"], row["wording"], row["order_question"],
                                                                                    row["cidade"], row["bairro"], row["sex"], row["answer"], 
                                                                                    row["research_name"], row["region"], row["question"], 
                                                                                    row["Faixa etária"], row["Religião"], row["votara"],
                                                                                    row["gestao_emanuel"], row["gestao_mauro"], row["gestao_lula"])
        values_row.append(value)

    db.executa_sql(sql + ", ".join(values_row))

results(0)
extra_results(0)
