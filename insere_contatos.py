import pg
import tempesta_excel

bd = pg.pg('167.86.105.252', 'unimed', 'postgres', '5432', 'Ganesha@1000')


xlsx = tempesta_excel.Excel('./dados.xlsx','dados')

line=2
sql =''
while xlsx.read_cell(line,1) != None:
    crm = xlsx.read_cell(line,1)
    nome = xlsx.read_cell(line,2)
    telefone = xlsx.read_cell(line, 3)
    inimigo = 0

    if xlsx.read_cell(line, 4) == True:
        inimigo = 1

    sql = sql + f"INSERT INTO public.contato(crm, nome_contato, telefones, inimigo) VALUES ('{crm}', '{nome}', '{telefone}', {inimigo});"
   
    print (line)
    line +=1

bd.executa_sql(sql)
xlsx.close_document()
bd.fechaBD()

