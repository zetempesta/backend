from fastapi import APIRouter
from transformation.fix_answers import fix_answers, fix_data


router = APIRouter()


@router.get("/fixanswer", response_model=bool)
async def fixData(idResearch:int):
    return fix_data(idResearch)