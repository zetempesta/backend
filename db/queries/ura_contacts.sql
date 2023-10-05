Select
    contact.id As id_contact,
    phone.phone_number
From
    contact Left Join
    respondent On respondent.id_contact = contact.id Inner Join
    contact_phone On contact_phone.contact = contact.id Inner Join
    phone On contact_phone.phone = phone.id
Where
    contact.city = 5103403 And
    (contact.source <> 'OAB' Or
        contact.source Is Null) And
    contact.robot Is Null And
    respondent.id_contact Is Null And
    phone.phone_number Like '(65) 9%' And
    Length(phone.phone_number) = 14