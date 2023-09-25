from fastapi import APIRouter
from schema.research import research
from models.research import get_research

router = APIRouter()

@router.get("/research", response_model=research)
async def listResearch(user:int):
    return get_research(user=user)