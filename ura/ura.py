from db.pg import pg
import csv
from db.db_conf import db_conf
from fastapi import File, UploadFile
from schema.ura import ura_results, ura_maillist
from typing import List
import re



conf = db_conf()
db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)


def get_ura_results(data: bytes)->bool:

    file_path ="./ura/ura_results.csv" 
    f = open(file_path, "wb")
    f.write(data)
    f.close()
    results = read_ura_results(file_path)
    ura_update_contact(results)    
    
    return True

def read_ura_results(file:str)->List[ura_results]:

    results = list()

    with open(file=file,encoding='latin_1') as file_obj: 
        counter = 0
        # Create reader object by passing the file  
        # object to reader method 
        reader_obj = csv.reader(file_obj) 
        
        # Iterate over each row in the csv  
        # file using reader object 
        next(reader_obj, None)

        for row in reader_obj:
            print(counter)
            print(row)
            counter+=1 
            columns = str(row).split(';')
            id = int(columns[16])
            status = columns[10]
            result = ura_results(id=id,status=status)
            results.append(result)
                           
        return results

def ura_update_contact(results:List[ura_results]):

    sql_command = list()
    

    for result in results:

        sql = """   UPDATE "public"."contact"
                    SET "robot" = '"""+ result.status +"""',
                        "last_update" = now()
                    WHERE
                        "id" = """+ str(result.id) 
        sql_command.append(sql)
    

    db.executa_sql(";".join(sql_command))

def contacts_for_ura(sql)->bool:
    contacts = db.consultar_db(sql)
    with open('/var/www/html/ura/mailing.csv', 'w', encoding='latin_1') as f:
        writer = csv.writer(f)
        line = "({};{};{};{};{};{};{};{};{};{})".format('|', 'telefone', '|', 'nome', 'endereco', 'cpf', 'TIPO', 'CLASSE','id', 'info')
        writer.writerow(line)
        for c in contacts:
            line = "({};{};{};{};{};{};{};{};{};{})".format('|', str(re.findall(r'\d+', c[1])), '|', 'nome', 'endereco', 'cpf', 'E', 'D',c[0], 'info')
            writer.writerow(line)
        






    
        