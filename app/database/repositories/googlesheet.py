import json
from pprint import pprint
from typing import List, Tuple

import asyncpg
from asyncpg import Pool, UniqueViolationError
from app.models import CounterpartyModel


class GoogleSheetRepository:
    def __init__(self, pool: Pool):
        self.pool = pool

    async def add_suppliers_data(self, data:List[CounterpartyModel]):
        """Выполняет массовую вставку/обновление"""
        records = []
        for value in data:
            records.append(
                (
                    value.id,
                    value.opf,
                    value.name,
                    value.supplier_category,
                    value.country,
                    value.inn,
                    value.tax_system,
                    value.reliability_level,
                    value.edo_operator,
                    value.contact_info,
                    value.responsible_person,
                    value.comment,
                    value.statutory_documents_link,
                    value.ka_guarantee_letter,
                    value.reliability_update_date,
                    value.card_details,
                    value.record_sheet_passport,
                    value.oi_guarantee_letter
                )
            )
        # SQL запрос с ON CONFLICT
        query = """
        INSERT INTO test.test_table (
            id, opf, name, supplier_category, country, inn, tax_system,
            reliability_level, edo_operator, contact_info, responsible_person,
            comment, statutory_documents_link, ka_guarantee_letter,
            reliability_update_date, card_details, record_sheet_passport,
            oi_guarantee_letter
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18
        )
        ON CONFLICT (id) DO UPDATE SET
            opf = EXCLUDED.opf,
            name = EXCLUDED.name,
            supplier_category = EXCLUDED.supplier_category,
            country = EXCLUDED.country,
            inn = EXCLUDED.inn,
            tax_system = EXCLUDED.tax_system,
            reliability_level = EXCLUDED.reliability_level,
            edo_operator = EXCLUDED.edo_operator,
            contact_info = EXCLUDED.contact_info,
            responsible_person = EXCLUDED.responsible_person,
            comment = EXCLUDED.comment,
            statutory_documents_link = EXCLUDED.statutory_documents_link,
            ka_guarantee_letter = EXCLUDED.ka_guarantee_letter,
            reliability_update_date = EXCLUDED.reliability_update_date,
            card_details = EXCLUDED.card_details,
            record_sheet_passport = EXCLUDED.record_sheet_passport,
            oi_guarantee_letter = EXCLUDED.oi_guarantee_letter
        """
        # Используем executemany для массовой вставки
        async with self.pool.acquire() as conn:
            await conn.executemany(query, records)
    async def get_suppliers_data(self):
        select_query = """
            SELECT * from test.test_table;
        """
        async with self.pool.acquire() as conn:
            result = await conn.fetch(select_query)
        return result