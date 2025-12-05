import datetime
from pprint import pprint
from typing import List, Dict, Any

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
        update_data = self.prepare_data_for_wild_insert(db_data=suppliers_data)
        pprint(update_data)

        await self.gs_connect(
            sheet = gs_params.sheet, spreadsheet = gs_params.spreadsheet, creds_json = settings.CREDS
        ).update_revenue_rows(update_data,table_id="№" )

    @staticmethod
    def prepare_data_for_wild_insert(
            db_data: List[Dict[str, Any]],
            key_field: str = "id"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Простая версия преобразования данных
        """

        FIELD_MAPPING = {
            'id': '№',
            'name': 'Наименование',
            'inn': 'ИНН',
            'contact_info': 'Контактные данные',
            'comment': 'Комментарий',
            'reliability_update_date': 'Дата обновления информации по благонадежности',
            'opf': 'ОПФ',
            'supplier_category': 'Категория поставщика',
            'country': 'Страна',
            'tax_system': 'Система налогообложения',
            'reliability_level': 'Уровень благонадежности',
            'edo_operator': 'Оператор ЭДО',
            'responsible_person': 'Ответственное лицо',
            'statutory_documents_link': 'Ссылка на уставные/личные документы',
            'ka_guarantee_letter': 'Гарантийное письмо КА',
            'card_details': 'Карточка / реквизиты',
            'record_sheet_passport': 'Лист записи/Паспорт',
            'oi_guarantee_letter': 'Гарантийное письмо ОИ',
            'check_1': 'Проверка',
            'check_2': 'Проверка №2'
        }

        result = {}

        for record in db_data:
            key_value = record.get(key_field)
            if not key_value:
                continue

            key = str(key_value)
            formatted = {}

            for db_field, sheet_header in FIELD_MAPPING.items():
                value = record.get(db_field)
                if value is None:
                    formatted[sheet_header] = ''
                else:
                    # Просто преобразуем в строку
                    formatted[sheet_header] = str(value).strip()

            result[key] = formatted

        return result