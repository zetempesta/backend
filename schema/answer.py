from pydantic import BaseModel
from schema.question import question
from typing import List
from typing import Optional

class person(BaseModel):
    idPerson: int
    name: str
    idCity:int
    idNeighborhood:Optional[int]
    sex:Optional[str]
    

class question(BaseModel):
    idQuestion:int
    type:str
    response:Optional[List[str]]
    
class answer(BaseModel):
    idResearch: int
    dontAnswer:Optional[bool]
    dontTalk:Optional[bool]
    person:person
    responses:List[question]