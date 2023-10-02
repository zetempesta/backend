from fastapi import APIRouter
from transformation.fix_answers import fix_answers


router = APIRouter()

@router.post("/fix_answer", response_model=bool)
async def postAnswer():
    return fix_answers()