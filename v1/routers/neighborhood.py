from fastapi import APIRouter
from schema.neighborhood import neighborhood
from models.neighborhood import get_neighborhood

router = APIRouter()

@router.get("/neighborhood", response_model=list[neighborhood])
async def listneighborhood():
    return get_neighborhood()