from fastapi import APIRouter
from schema.city import city
from models.city import get_cities

router = APIRouter()

@router.get("/city", response_model=list[city])
async def listCity():
    return get_cities()