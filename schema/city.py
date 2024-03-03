from pydantic import BaseModel
    
class city(BaseModel):
    id: int
    name: str