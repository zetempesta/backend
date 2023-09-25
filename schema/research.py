from pydantic import BaseModel
from schema.person import person
from schema.question import question
from typing import List


    
class research(BaseModel):
    idResearch: int
    name: str
    person:person
    questions:List[question]