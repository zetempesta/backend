from pydantic import BaseModel

class surveyDB(BaseModel):
    id: int
    name: str