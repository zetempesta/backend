from pydantic import BaseModel
from schema.person import person
from schema.question import question
from typing import List
from typing import Optional
from datetime import date

    
class research(BaseModel):
    idResearch: int
    name: str
    person:person
    questions:List[question]

class researchDB(BaseModel):
    id:int
    name:str
    meta:int
    begin_date:date
    end_date:date
    valid:bool