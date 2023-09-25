from pydantic import BaseModel
from typing import List
from schema.responseOptions import responseOption

class question(BaseModel):
    idQuestion: int
    titleQuestion: str
    type: str
    responseOptions:List[responseOption]