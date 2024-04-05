from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime as dt

class Call(BaseModel):
    id: int
    id_research: int   
    id_user: int
    call_time: Optional[dt] = dt.now()
    id_contact:int