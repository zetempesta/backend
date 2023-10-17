from pydantic import BaseModel
from typing import List
from schema.responseOptions import responseOption

class question(BaseModel):
    idQuestion: int
    titleQuestion: str
    type: str
    responseOptions:List[responseOption]


class questionDB(BaseModel):
    id: int
    survey: int
    wording:str
    order_question:int
    null_answer:str
    formtype:str
    