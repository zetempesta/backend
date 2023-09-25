from pydantic import BaseModel

class participant(BaseModel):
    id_research: int
    id_contact: int