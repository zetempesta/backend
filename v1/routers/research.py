from fastapi import APIRouter
from schema.research import research, researchDB
from models.research import get_research, post_research

router = APIRouter()

@router.get("/research", response_model=research)
async def listResearch(user:int):
    return get_research(user=user)

@router.post("/research", response_model=int)
async def listResearch(r:researchDB):
    return post_research(r)