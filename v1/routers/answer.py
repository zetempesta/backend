from fastapi import APIRouter
from schema.answer import answer as answer_schema
from models.answer import postAnswer as pa


router = APIRouter()

@router.post("/answer", response_model=bool)
async def postAnswer(answer:answer_schema):
    return pa(answer)