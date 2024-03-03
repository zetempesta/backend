import pg
from db.db_conf import db,openDB

def get_cities():
    openDB()
    query =  db.consultar_db('Select city.id, city.name From city Order By city.name')
    db.fechaBD()
    return query

#def consulta_usuario(usuario, senha):


def insere_sessao(sessao, usuario):
    try:
        bd = pg.pg('167.86.105.252', 'unimed', 'postgres', '5432', 'Ganesha@1000')        
        bd.executa_sql(f"INSERT INTO public.sessao(sessao, usuario) VALUES ('{sessao}', '{usuario}');")
        bd.fechaBD()
        return True
    except:
        return False


#TODO Incluir erro caso não tenha encontrado participante
def get_participante_livre():
    sql = '''select id_contato, id_pesquisa vw_participante_livre'''
    query = bd.consultar_db(sql)

    if len(query)>0:
            id_contato = str(query[0][0])
            id_pesquisa = str(query[0][1])



    

    


def atualiza_resposta(valores):
    print('atualiza_resposta')
    print(valores)
    bd = pg.pg('167.86.105.252', 'unimed', 'postgres', '5432', 'Ganesha@1000')
    sql = "UPDATE public.resposta "
    sql = sql + f" SET q1='{valores[1]}', q2='{valores[2]}', q3='{valores[3]}', q4='{valores[4]}',"
    sql = sql + f" q5='{valores[5]}', q6='{valores[6]}', q7='{valores[7]}', q8='{valores[8]}', data_hora_2=now(),"
    sql = sql + f" atendeu={valores[9]}, respondeu={valores[10]}"
    sql = sql + f" WHERE crm='{valores[0]}';"
    print(sql)
    bd.executa_sql(sql)
    bd.fechaBD()


def busca_contato(sessao):
    usuario = ''
    crm = ''
    retorno = ''
    valores=[]


    bd = pg.pg('167.86.105.252', 'unimed', 'postgres', '5432', 'Ganesha@1000')
    sql = f"Select sessao.usuario From sessao Where sessao.sessao = '{sessao}'"

    query = bd.consultar_db(sql)

    if len(query)>0:
        usuario = str(query[0][0])
        valores.append(usuario)

    sql = "Select contato.crm, contato.nome_contato, contato.telefones From contato Where contato.crm Not In (Select resposta.crm From resposta) Order By contato.inimigo  limit 1"
    query = bd.consultar_db(sql)

    if len(query)>0:
        crm = str(query[0][0])
        nome = str(query[0][1])
        telefone = str(query[0][2])
                
        valores.append(crm)
        valores.append(nome)
        valores.append(telefone)    
    
    sql = f"INSERT INTO public.resposta(crm, usuario) VALUES ('{crm}', '{usuario}');"
    bd.executa_sql(sql)

    retorno = "¬".join(valores)
    bd.fechaBD()

    return retorno