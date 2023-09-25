from db.pg import pg
from typing import List
from schema.answer import answer as ar
from db.db_conf import db_conf

conf = db_conf()


def postAnswer(answer:ar)->bool:
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
    print(answer)
    sql = """UPDATE "public"."respondent" SET
    	        "dont_pickup" = """ + str(answer.dontAnswer) + """,
	            "dont_accept" = """ + str(answer.dontTalk)  + """
            WHERE
	            "id_contact" = """ + str(answer.person.idPerson) + """ AND
	            "id_research" = """ + str(answer.idResearch) 
    db.executa_sql(sql)
    
    sql = """UPDATE "public"."contact" SET
                name = '""" + str(answer.person.name) + """',
                city = """ + str(answer.person.idCity) + """,
                sex = '""" + str(answer.person.sex) + """',
	            neighborhood = """ + str(answer.person.idNeighborhood) + """
            WHERE
                id = """ + str(answer.person.idPerson)
    db.executa_sql(sql)


    for r in answer.responses:
        response_value=''
        match r.type:
            case "radioButton":
               if r.response != None:
                   response_value = r.response[0]
            case "textArea":
               if r.response != None:
                   response_value = r.response[0]
            case "checkbox":
               if r.response != None:
                   response_value = '¬'.join(r.response)

            case "comboBox":
               if r.response != None:
                   response_value = r.response[0]
        
        sql = """INSERT INTO public.answer (research, question, date_time_end, contato, answer)
                 VALUES (
                    """ + str(answer.idResearch) + """, """ + str(r.idQuestion) + """, now(), """ + str(answer.person.idPerson) + """, '""" + str(response_value) + """' )
                 ON CONFLICT ON CONSTRAINT resposta_pkey
                 DO
                 UPDATE SET
                    date_time_end = now(),
                    answer = '""" + str(response_value) + """'"""
       
        db.executa_sql(sql)

    return True