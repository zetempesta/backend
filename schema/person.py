from pydantic import BaseModel
from typing import List, Optional

class person(BaseModel):
    idPerson: int
    name: str   
    idCity: int
    idNeighboor:Optional[int]
    phones:List[str]
    sex:Optional[str]
    