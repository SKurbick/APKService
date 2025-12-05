import datetime
from pprint import pprint
from typing import List

from app.infrastructure.googlesheet import PCGoogleSheet
from app.models import GoogleSheetParams #GoogleSheetData
from app.database.repositories import GoogleSheetRepository
from app.models import GoogleSheetParams
from app.models.googlesheet import dataframe_to_models
from config import settings

class GoogleSheetService:
    def __init__(
            self,
            google_sheet_repository: GoogleSheetRepository,
    ):
        self.gs_connect = PCGoogleSheet
        self.google_sheet_repository = google_sheet_repository

    async def add_suppliers_data_in_db(self, gs_params: GoogleSheetParams):

        suppliers_data = self.gs_connect(
            sheet = gs_params.sheet, spreadsheet = gs_params.spreadsheet, creds_json = settings.CREDS
        ).get_suppliers_data()
        data = dataframe_to_models(suppliers_data)

        for v in data:
            pprint(v.model_dump())

        print(len(data))
        add_suppliers_data = await self.google_sheet_repository.add_suppliers_data(data)


    async def get_suppliers_data_from_db(self, gs_params: GoogleSheetParams):
        suppliers_data = await self.google_sheet_repository.get_suppliers_data()
        update_data = {}


        pprint(update_data)
        self.gs_connect(
            sheet = gs_params.sheet, spreadsheet = gs_params.spreadsheet, creds_json = settings.CREDS
        ).add_suppliers_data()