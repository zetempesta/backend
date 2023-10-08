from db.sqlite import sqlite
from db.pg import pg
from db.db_conf import db_conf
import re


conf = db_conf()
db = pg(conf.host, conf.database,conf.user, conf.port, conf.password)

sq = sqlite()
def create_phone_table():
    print('create_phone_table()')
    sql = """CREATE TABLE "phone" ( 
            "id" BigInt,
            "prefixo" SmallInt,
            "telefone" BigInt,
            "phone_number" varchar(200),
            "phone_type" varchar(1), valid_phone [BOOLEAN] DEFAULT True,
            PRIMARY KEY ( "id" ) )"""

    sq.conn.execute(sql)
    sq.conn.commit()

def read_phones():
    print('read_phones()')
    contacts = db.consultar_db('SELECT	"id", "phone_number" FROM "public"."phone"')

    sql = "insert into phone (id, phone_number) values (?,?)"
    sq.conn.executemany(sql, contacts)    
    sq.conn.commit()


def drop_phones_table():
    print('drop_phones_table()')
    sql = "drop table phone"
    sq.conn.execute(sql)
    sq.conn.commit()

def duplicate_table_phone():
    print('duplicate_table_phone()')

    sql = "drop table fixed_phone"
    sq.conn.execute(sql)
    sq.conn.commit()

    sql = """CREATE TABLE "fixed_phone" ( 
            "id" BigInt,
            "prefixo" SmallInt,
            "telefone" BigInt,
            "phone_number" varchar(200),
            "original_number" varchar(200),
            "phone_type" varchar(1), valid_phone [BOOLEAN] DEFAULT True,
            PRIMARY KEY ( "id" ) )"""
    sq.conn.execute(sql)
    sq.conn.commit()

    sql = "insert into fixed_phone(id, prefixo, telefone, original_number, phone_type, valid_phone) select * from phone"
    sq.conn.execute(sql)
    sq.conn.commit()

def remove_characters(text:str)->str:
    text = text.replace(' ','')
    text = text.replace('-','')
    text = text.replace('(','')
    text = text.replace(')','')
    l = re.findall("\d+", text)
    if len(l) > 0:
        text = l[0]
    return text


def fix():
    print('Getting phones')
    phones_list = sq.query("Select fixed_phone.id, fixed_phone.phone_number, length(fixed_phone.phone_number) as tamanho From fixed_phone where phone_type is null")
    phones_fixed = list()
    dictTamanho = dict()

    print('Fixing Phones')
    for p in phones_list:
        id = p[0]
        phone_number = remove_characters(p[1])
        tamanho =len(phone_number)
        ddd=None
        phone=None
        phone_type=None
        fixed_phone = list()

        dictTamanho[str(tamanho)] = tamanho
        if tamanho == 11:
            if phone_number[2] == '9':
                ddd = phone_number[0:2]
                phone = phone_number[2:]
                phone_type = 'M'
                fixed_phone.append(ddd)
                fixed_phone.append(phone)
                fixed_phone.append(phone_number)
                fixed_phone.append(phone_type)
                fixed_phone.append(id)
                phones_fixed.append(fixed_phone)
            
            
        if tamanho == 10:
            if phone_number[2] == '9':
                print('Fixing Mobiles')
                ddd = phone_number[0:2]
                phone = phone_number[2:]
                formated_phone = "{}9{}".format(ddd, phone)
                phone_number = formated_phone
                phone_type = 'M'
                fixed_phone.append(ddd)
                fixed_phone.append(phone)
                fixed_phone.append(phone_number)
                fixed_phone.append(phone_type)
                fixed_phone.append(id)
                phones_fixed.append(fixed_phone)
        
        if tamanho <= 6:
            ddd = ''
            phone = ''
            formated_phone = ''
            phone_number = ''
            phone_type = 'I'
            fixed_phone.append(ddd)
            fixed_phone.append(phone)
            fixed_phone.append(phone_number)
            fixed_phone.append(phone_type)
            fixed_phone.append(id)
            phones_fixed.append(fixed_phone)

        if tamanho == 8:
            if phone_number[0]=='9' or phone_number[0]=='8':
                ddd = '65'
                phone = phone_number
                formated_phone = ddd + '9' + phone_number
                phone_number = formated_phone
                phone_type = '8'
                fixed_phone.append(ddd)
                fixed_phone.append(phone)
                fixed_phone.append(phone_number)
                fixed_phone.append(phone_type)
                fixed_phone.append(id)
                phones_fixed.append(fixed_phone)
            else:
                ddd = '65'
                phone = phone_number
                formated_phone = ddd + phone_number
                phone_number = formated_phone
                phone_type = '8'
                fixed_phone.append(ddd)
                fixed_phone.append(phone)
                fixed_phone.append(phone_number)
                fixed_phone.append(phone_type)
                fixed_phone.append(id)
                phones_fixed.append(fixed_phone)
        
        if tamanho == 9:
            if phone_number[2]=='9' or phone_number[2]=='8':
                ddd = phone_number[0:2]
                phone = phone_number[2:]
                formated_phone = ddd + '99' + phone_number
                phone_number = formated_phone
                phone_type = '9'
                fixed_phone.append(ddd)
                fixed_phone.append(phone)
                fixed_phone.append(phone_number)
                fixed_phone.append(phone_type)
                fixed_phone.append(id)
                phones_fixed.append(fixed_phone)
            else:
                ddd = phone_number[0:2]
                phone = phone_number[2:]
                formated_phone = ddd + '3' + phone_number
                phone_number = formated_phone
                phone_type = '9'
                fixed_phone.append(ddd)
                fixed_phone.append(phone)
                fixed_phone.append(phone_number)
                fixed_phone.append(phone_type)
                fixed_phone.append(id)
                phones_fixed.append(fixed_phone)
        

    sql = """update 
    fixed_phone 
    set 
    prefixo = ?,
    telefone = ?,
    phone_number = ?,
    phone_type = ?
    where 
    id = ?;"""

    print(len(phones_fixed))
    sq.conn.executemany(sql, phones_fixed)
    sq.conn.commit()

    t = list(dictTamanho.items())
    t.sort()
    print(t)

# drop_phones_table()
# create_phone_table()
# read_phones()
# duplicate_table_phone()
# fix()