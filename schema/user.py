from pydantic import BaseModel
    
class user(BaseModel):
    id:int
    mail:str
    name:str
    username:str

class login(BaseModel):
    username:str
    password:str    

class db_user(BaseModel):
    id:int
    mail:str
    name:str
    username:str
    password:str