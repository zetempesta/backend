from pydantic import BaseModel
    
class neighborhood(BaseModel):
    id: int
    name: str
    city:int