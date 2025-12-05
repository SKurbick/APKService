from typing import List
from fastapi import APIRouter, Depends, status, Body, HTTPException, Query

from app.models import GoogleSheetParams
from app.dependencies import get_googlesheet_service
from app.service import GoogleSheetService
router = APIRouter(prefix="/googlesheet", tags=["Работа с гугл таблицей"])

"""
{
  "sheet": "База контрагентов",
  "spreadsheet": "TEST: УУ - Дирекция",
  "table_id_header": "string"
}
"""
@router.post("/get_data_in_sheet")
async def get_data_in_sheet(
        gs_params: GoogleSheetParams,
        service: GoogleSheetService =  Depends(get_googlesheet_service)
):
    await service.add_suppliers_data_in_db(gs_params=gs_params)
    print("OK")
    return {"message": "OK"}


@router.post("/add_data_in_sheet")
async def add_data_in_sheet(
):
    print("OK")
    return {"message": "OK"}
