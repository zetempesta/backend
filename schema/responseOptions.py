from pydantic import BaseModel

class responseOption(BaseModel):
    titleResponse: str
    valueResponse: str
    