from fastapi import APIRouter, File, UploadFile
from typing import Annotated
from ura.ura import get_ura_results, contacts_for_ura



router = APIRouter()

@router.post("/uraresults", response_model=bool)
async def upload_ura_results(file: UploadFile):
    file_bytes = await file.read()
    return get_ura_results(file_bytes)

@router.post("/uracontacts", response_model=bool)
async def create_ura_contacts(SQL: str):
    return contacts_for_ura(SQL)
