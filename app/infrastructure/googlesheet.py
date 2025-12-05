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

    @staticmethod
    def get_column_letter(col_idx: int) -> str:
        """Конвертирует индекс колонки в букву (A, B, C, ...)"""
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def insert_data_correct(self, data_dict: dict, sheet_header="id") -> None:
        """
        Оптимизированная версия - обновляет данные целыми столбцами.
        """
        try:
            # Получаем заголовки таблицы
            headers = self.sheet.row_values(1)
            # print(headers)
            # Находим индекс колонки wild
            wild_col_idx = None
            for idx, header in enumerate(headers):
                if sheet_header in header.lower():
                    wild_col_idx = idx
                    print(wild_col_idx)

            if wild_col_idx is None:
                # logger.error(f"Колонка {sheet_header} не найдена в таблице")
                print(f"Колонка {sheet_header} не найдена в таблице")

                return

            # Находим индексы и диапазон наших целевых колонок
            # target_headers = list(next(iter(data_dict.values())).keys()) if data_dict else []
            target_headers = list(set().union(*(item.keys() for item in data_dict.values())))
            print(f"Все целевые заголовки: {target_headers}")

            target_indices = []

            for header in target_headers:
                if header in headers:
                    target_indices.append(headers.index(header))

            if not target_indices:
                # logger.error("Целевые заголовки не найдены в таблице")
                print("Целевые заголовки не найдены в таблице")

                return

            # Сортируем индексы и проверяем, что они идут подряд
            target_indices.sort()
            is_consecutive = all(target_indices[i] + 1 == target_indices[i + 1]
                                 for i in range(len(target_indices) - 1))

            # Получаем все данные таблицы
            all_data = self.sheet.get_all_values()

            # Создаем матрицу для обновления (строки x колонки)
            updates = []

            if is_consecutive and len(target_indices) > 1:
                # ОПТИМИЗАЦИЯ: обновляем целым диапазоном столбцов
                start_col = target_indices[0]
                end_col = target_indices[-1]

                # ПРАВИЛЬНО формируем диапазон: "AX2:BA5886"
                start_col_letter = self.get_column_letter(start_col + 1)
                end_col_letter = self.get_column_letter(end_col + 1)
                update_range = f"{start_col_letter}2:{end_col_letter}{len(all_data)}"

                # logger.info(f"Обновляем диапазон: {update_range}")
                print(f"Обновляем диапазон: {update_range}")

                # Создаем матрицу обновлений
                update_matrix = [['' for _ in range(len(target_indices))] for _ in range(len(all_data) - 1)]

                # Заполняем матрицу данными
                for row_idx in range(1, len(all_data)):
                    row = all_data[row_idx]
                    if len(row) > wild_col_idx:
                        current_wild = row[wild_col_idx]
                        if current_wild in data_dict:
                            wild_data = data_dict[current_wild]
                            for i, col_idx in enumerate(target_indices):
                                header = headers[col_idx]
                                if header in wild_data:
                                    update_matrix[row_idx - 1][i] = wild_data[header]
                                else:
                                    # Сохраняем оригинальное значение если нет в словаре
                                    update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                        else:
                            # Сохраняем оригинальные значения для строк без совпадения
                            for i, col_idx in enumerate(target_indices):
                                update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                    else:
                        # Для строк без wild данных
                        for i, col_idx in enumerate(target_indices):
                            update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''

                updates.append({
                    'range': update_range,
                    'values': update_matrix
                })
            else:
                # Если колонки не подряд, обновляем каждую колонку отдельно
                for col_idx in target_indices:
                    header = headers[col_idx]
                    col_letter = self.get_column_letter(col_idx + 1)
                    # ПРАВИЛЬНЫЙ формат: "AX2:AX5886"
                    col_range = f"{col_letter}2:{col_letter}{len(all_data)}"

                    # logger.info(f"Обновляем колонку: {col_range}")
                    print(f"Обновляем колонку: {col_range}")

                    # Подготавливаем данные для столбца
                    column_data = []
                    for row_idx in range(1, len(all_data)):
                        row = all_data[row_idx]
                        if len(row) > wild_col_idx:
                            current_wild = row[wild_col_idx]
                            if current_wild in data_dict and header in data_dict[current_wild]:
                                column_data.append([data_dict[current_wild][header]])
                            else:
                                column_data.append([row[col_idx] if col_idx < len(row) else ''])
                        else:
                            column_data.append([row[col_idx] if col_idx < len(row) else ''])

                    updates.append({
                        'range': col_range,
                        'values': column_data
                    })

            # pprint(updates)

            # Выполняем обновление
            if updates:
                for i, update in enumerate(updates):
                    try:
                        self.sheet.update(update['range'], update['values'], value_input_option='USER_ENTERED')
                        # logger.info(f"Успешно обновлен диапазон {update['range']} ({i + 1}/{len(updates)})")
                        print(f"Успешно обновлен диапазон {update['range']} ({i + 1}/{len(updates)})")

                    except Exception as e:
                        print(f"Ошибка при обновлении {update['range']}: {e}")

                        # logger.error(f"Ошибка при обновлении {update['range']}: {e}")
                        # Можно добавить повторные попытки или продолжить

        except Exception as e:
            # logger.error(f"Ошибка при вставке данных: {e}")
            print(f"Ошибка при вставке данных: {e}")

            raise


    @retry_on_quota_exceeded_async()
    async def update_revenue_rows(self, data_json, table_id="Артикул"):
        # Получаем текущие данные из таблицы
        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Если DataFrame пустой (только заголовки или вообще ничего)
        if df.empty or table_id not in df.columns:
            # Таблица пустая, все артикулы будут новыми
            existing_articles = set()
        else:
            # Таблица не пустая, получаем существующие артикулы
            existing_articles = set(df[table_id].dropna().unique())

        # Преобразуем входные данные
        json_df = pd.DataFrame.from_dict(data_json, orient='index')
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # 1. Добавляем новые артикулы, которых нет в таблице
        new_articles = [art for art in json_df.index if art not in existing_articles]

        if new_articles:
            # Получаем заголовки
            if df.empty:
                # Если таблица полностью пустая, получаем заголовки из листа
                headers = self.sheet.row_values(1)  # Первая строка с заголовками
            else:
                headers = df.columns.tolist()

            new_rows = []

            for article in new_articles:
                new_row = {col: "" for col in headers}  # Пустая строка
                new_row[table_id] = article  # Устанавливаем артикул

                # Заполняем доступные данные из json_df
                for col in json_df.columns:
                    if col in headers:
                        new_row[col] = json_df.at[article, col]

                new_rows.append([new_row.get(col, "") for col in headers])

            # Вставляем новые строки
            self.sheet.append_rows(new_rows)

        # 2. Обновляем существующие данные (только если таблица не пустая)
        if not df.empty and table_id in df.columns:
            updates = []
            headers = df.columns.tolist()

            for index, row in json_df.iterrows():
                matching_rows = df[df[table_id] == index].index
                for idx in matching_rows:
                    row_number = idx + 2
                    for column in row.index:
                        if column in headers:
                            column_index = headers.index(column) + 1
                            column_letter = column_index_to_letter(column_index)
                            updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})

            if updates:
                self.sheet.batch_update(updates)