insert into respondent(id_contact,id_research)
Select
    contact.id as id_contact,
    '0' as id_research
From
    contact Left Join
    respondent On respondent.id_contact = contact.id
Where
    contact.city = 5103403 And
    (contact.source <> 'OAB' Or
        contact.source Is Null) And
    contact.robot In ('Não atendida', 'Chamou e não atendeu', 'Atendido', 'Caixa postal', 'Ocupado') And
    respondent.id_contact Is Null