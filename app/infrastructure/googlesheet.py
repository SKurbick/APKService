import asyncio
import time
from datetime import datetime

import pandas

import gspread
import requests
from gspread import Client, service_account
import pandas as pd


def column_index_to_letter(index):
    letter = ''
    while index > 0:
        index -= 1
        letter = chr((index % 26) + 65) + letter
        index //= 26
    return letter

def retry_on_quota_exceeded(max_retries=10, delay=60):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    print(f"Error: {e} | time sleep 60 sec [сработал декоратор]")
                    time.sleep(delay)
                    retries += 1
            print("Не удалось выполнить операцию после нескольких попыток.")
            raise Exception("Не удалось выполнить операцию после нескольких попыток.")

        return wrapper

    return decorator


def retry_on_quota_exceeded_async(max_retries=10, delay=60):
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    print(f"Error: {e} | Async sleep {delay} sec [сработал декоратор]")
                    await asyncio.sleep(delay)
                    retries += 1
            print("Не удалось выполнить операцию после нескольких попыток.")
            raise Exception("Не удалось выполнить операцию после нескольких попыток.")

        return async_wrapper

    return decorator


class PCGoogleSheet:
    def __init__(self, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.creds_json = creds_json
        self.spreadsheet = spreadsheet
        self.client = self.client_init_json()
        self.sheet = self.connect_to_sheet(sheet)

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    def connect_to_sheet(self, sheet: str):
        """Попытка подключения к Google Sheets с повторными попытками в случае ошибки."""
        for _ in range(10):
            try:
                spreadsheet = self.client.open(self.spreadsheet)
                return spreadsheet.worksheet(sheet)
            except (gspread.exceptions.APIError, requests.exceptions.ConnectionError) as e:
                print(f"Error: {e} | Время: {datetime.now()} | Time sleep: 60 sec")
                time.sleep(60)
        print("Не удалось подключиться к Google Sheets после 10 попыток.")
        raise Exception("Не удалось подключиться к Google Sheets после 10 попыток.")

    def add_suppliers_data(self):
        pass

    def get_suppliers_data(self) -> pd.DataFrame:
        """
        Получает все данные из таблицы поставщиков и преобразует в DataFrame.

        Предполагается, что заголовки столбцов находятся в первой строке.
        Возвращает:
            DataFrame с данными поставщиков
        """
        try:
            # Получаем все значения из листа
            all_data = self.sheet.get_all_values()

            if not all_data or len(all_data) < 2:  # Проверяем, есть ли данные
                print("Таблица пуста или содержит только заголовки")
                return pd.DataFrame()

            # Первая строка - заголовки
            headers = all_data[0]

            # Остальные строки - данные
            data_rows = all_data[1:]

            # Создаем DataFrame
            df = pd.DataFrame(data_rows, columns=headers)

            # Убираем пустые строки (если все значения NaN)
            df = df.dropna(how='all')

            # Преобразуем названия столбцов в более удобный формат (опционально)
            # df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

            print(f"Получено {len(df)} записей из Google Sheets")
            return df

        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            raise