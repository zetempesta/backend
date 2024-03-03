from pydantic import BaseModel
    
class ura_results(BaseModel):
    id: int
    status: str

class ura_maillist(BaseModel):
    firstbar:str
    telefone:int
    secondbar:str
    nome:str
    endereco:str
    cpf:str
    tipo:str
    classe:str
    id:int
