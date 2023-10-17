from pydantic import BaseModel
from schema.person import person
from schema.question import question
from typing import List
from typing import Optional
from datetime import date

    
class optionDB(BaseModel):
    id: int
    question:int
    title: str
    value:str
    option_order:int