from db.pg import pg
from typing import List
from schema.research import research, researchDB
from schema.question import question
from schema.person import person
from schema.responseOptions import responseOption
from schema.participant import participant
from models.call import newCall
from db.db_conf import db_conf
conf = db_conf()

def get_research(user:int)->research:
    
    p = get_participant(user)
    set_operator(user, p.id_contact, p.id_research)
    questions = get_questions(p.id_research)
    person_data = get_person(p.id_contact)

    #TODO Incluir o nome da pesquisa no retorno
    newCall(id_user=user, id_research=p.id_research, id_contact=p.id_contact)
    return research(idResearch=p.id_research,name='',person=person_data,questions=questions)

def set_operator(operator:int, id_contact:int, id_research):
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
    sql = """
    update 
        respondent 
    set 
    user_operator = """ + str(operator) + """,
    status_contact = 1
    where id_contact = """ + str(id_contact) + """ and id_research = """ + str(id_research)
    db.executa_sql(sql)

def get_person(contact:int)->person:
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
    sql ="""Select
                contact_phone.contact As idPerson,
                contact.name,
                contact.city As idCity,
                contact.neighborhood,
                phone.phone_number
            From
                contact_phone Inner Join
                phone On contact_phone.phone = phone.id Inner Join
                contact On contact_phone.contact = contact.id
            Where
                contact_phone.contact = """ + str(contact)

    query = db.consultar_db(sql)
        
    contact_data = list(dict.fromkeys([sublist[0:4] for sublist in query]))
    phones = [sublist[4] for sublist in query]

    return person(idPerson=contact_data[0][0],name=contact_data[0][1],idCity=contact_data[0][2],idNeighboor=contact_data[0][3],phones=phones,sex='')
    
def get_questions(research_id:int)->List[question]:
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)

    questions = list()

    sql = """ Select
            question.id As idQuestion,
            question.wording As titleQuestion,
            question.formtype As type,
            question_option.title As titleResponse,
            question_option.value As valueResponse
        From
            question Inner Join
            survey On question.survey = survey.id Inner Join
            research_survey On research_survey.survey = survey.id Inner Join
            question_option On question_option.question = question.id
        Where
            research_survey.research = """ + str(research_id) + """
        Order By
            research_survey.order_survey,
            question.order_question,
            question_option.option_order"""
    
       
    query = db.consultar_db(sql)

    questionList = list(dict.fromkeys([sublist[0:3] for sublist in query]))

    for q in questionList:

        filtered = list(filter(lambda c: c[0]==q[0],query))
        options = list(dict.fromkeys([sublist[3:5] for sublist in filtered]))
        response_option_list = list()

        for o in options:
            ro = responseOption(titleResponse=o[0],valueResponse=o[1])
            response_option_list.append(ro)
        
        q_item = question(idQuestion=q[0],titleQuestion=q[1],type=q[2],responseOptions=response_option_list)
        questions.append(q_item)
    
    return questions
        
def get_participant(user:int)-> participant:

    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
    query = db.consultar_db(f"""
                                Select
                                        research.public.respondent.id_contact,
                                        research.public.respondent.id_research
                                    From
                                        respondent Inner Join
                                        research On research.public.respondent.id_research = research.id Inner Join
                                        research_user On research_user.id_research = research.id
                                    Where
                                        research_user.id_user = {user} And
                                        research.valid And
                                        research.public.respondent.status_contact = 0
                                    ORDER BY random()
                                    Limit 1""")
    
    return participant( id_contact=query[0][0],id_research= query[0][1])

def post_research(r:researchDB)->int:
    db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)
  
    sql ="select nextval('research_id_seq') as ID"
  
    db.consultar_db(sql=sql)
  
    id = db.consultar_db(sql=sql)[0][0]
  
    sql = """INSERT INTO "public"."research" ( "name", "begin_date", "end_date", "id", "valid", "meta") 
                VALUES ( '{}', '{}', '{}', {}, false, {} )""".format(r.name, r.begin_date.isoformat(), r.end_date.isoformat(), id, r.meta)
    return int(id)


