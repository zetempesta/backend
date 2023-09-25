import tempesta_excel


xlsx = tempesta_excel.Excel('./coop.xlsx','FULL')
coop = {}
line = 1

while xlsx.read_cell(line,1) != None:
    line+=1
    crm = xlsx.read_cell(line,2)
    name = str(xlsx.read_cell(line,3))
    phone = str(xlsx.read_cell(line,4))
    enemy = False

    if xlsx.read_cell(line,5)!= None:
        enemy = True

    data = [name,phone,enemy]
    coop[crm] = data

xlsx.close_document()

print('coop')
xlsx = tempesta_excel.Excel('./novo.xlsx','NOVOS')
novos = {}
line = 1

while xlsx.read_cell(line,1) != None:
    line+=1
    crm = xlsx.read_cell(line,1)
    name = str(xlsx.read_cell(line,2))
    phone = str(xlsx.read_cell(line,3))
   

    data = [name,phone,False]
    novos[crm] = data

xlsx.close_document()

print('novos')
for k in novos.keys():    
    if k in coop:
        name = coop[k][0]
        phone = coop[k][1]  + "¬" + novos[k][1]
        enemy = coop[k][2]
        data = [name,phone,enemy]
        coop[k] = data
    else:
        coop[k] = novos[k]

print('relacao')
xlsx = tempesta_excel.Excel('./dados.xlsx','dados')

line=1
for k in coop.keys():
    line+=1
    xlsx.write_cell(line,1, k)
    xlsx.write_cell(line,2, coop[k][0])
    xlsx.write_cell(line,3, coop[k][1])
    xlsx.write_cell(line,4, coop[k][2])
    print(line)

xlsx.save()
xlsx.close_document()
print('finalizou')


