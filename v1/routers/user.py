from fastapi import APIRouter
from schema.user import user
from schema.user import login as lg
from models.user import get_user as gt


router = APIRouter()

@router.post("/login", response_model=user)
async def login(login_data:lg):
    return gt(login_data.username, login_data.password)