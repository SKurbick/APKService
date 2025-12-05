import pandas as pd
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import date


class GoogleSheetParams(BaseModel):
    sheet: str
    spreadsheet: str
    table_id_header: str


from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, ClassVar
from datetime import date
import pandas as pd


class CounterpartyModel(BaseModel):
    """Модель для данных контрагента с маппингом на поля БД"""

    # Основные поля
    id: int = Field(..., alias="№", description="Уникальный идентификатор записи")
    name: str = Field(..., alias="Наименование", description="Наименование организации")
    inn: Optional[str] = Field(None, alias="ИНН", description="Идентификационный номер налогоплательщика")
    contact_info: Optional[str] = Field(None, alias="Контактные данные", description="Контактные данные")
    comment: Optional[str] = Field(None, alias="Комментарий", description="Комментарий")
    reliability_update_date: Optional[date] = Field(
        None,
        alias="Дата обновления информации по благонадежности",
        description="Дата обновления информации по благонадежности"
    )

    # Справочные поля
    opf: Optional[str] = Field(None, alias="ОПФ", description="Организационно-правовая форма")
    supplier_category: Optional[str] = Field(None, alias="Категория поставщика", description="Категория поставщика")
    country: Optional[str] = Field(None, alias="Страна", description="Страна регистрации")
    tax_system: Optional[str] = Field(None, alias="Система налогообложения", description="Система налогообложения")
    reliability_level: Optional[str] = Field(None, alias="Уровень благонадежности", description="Уровень благонадежности")
    edo_operator: Optional[str] = Field(None, alias="Оператор ЭДО", description="Оператор электронного документооборота")
    responsible_person: Optional[str] = Field(None, alias="Ответственное лицо", description="Ответственное лицо")

    # Дополнительные поля
    statutory_documents_link: Optional[str] = Field(
        None,
        alias="Ссылка на уставные/личные документы",
        description="Ссылка на уставные/личные документы"
    )
    ka_guarantee_letter: Optional[str] = Field(
        None,
        alias="Гарантийное письмо КА",
        description="Гарантийное письмо КА"
    )
    card_details: Optional[str] = Field(
        None,
        alias="Карточка / реквизиты",
        description="Карточка/реквизиты"
    )
    record_sheet_passport: Optional[str] = Field(
        None,
        alias="Лист записи/Паспорт",
        description="Лист записи/Паспорт"
    )
    oi_guarantee_letter: Optional[str] = Field(
        None,
        alias="Гарантийное письмо ОИ",
        description="Гарантийное письмо ОИ"
    )
    check_1: Optional[str] = Field(None, alias="Проверка")
    check_2: Optional[str] = Field(None, alias="Проверка №2")

    # Конфигурация Pydantic V2
    model_config = ConfigDict(
        populate_by_name=True,  # замена allow_population_by_field_name
        str_strip_whitespace=True,  # автоматически убирает пробелы в строках
    )

    @field_validator('reliability_update_date', mode='before')
    @classmethod
    def parse_date(cls, v):
        """Преобразует строку в дату (работает до валидации)"""
        if v is None or v == '':
            return None
        try:
            return pd.to_datetime(v, errors='coerce', dayfirst=True).date()
        except:
            return None

    @field_validator('id', mode='before')
    @classmethod
    def parse_id(cls, v):
        """Преобразует различные форматы ID в int"""
        if v is None or v == '':
            return None
        try:
            if isinstance(v, float):
                return int(v)
            elif isinstance(v, str):
                return int(float(v.strip())) if v.strip() else None
            else:
                return int(v)
        except (ValueError, TypeError):
            return None


def dataframe_to_models(df: pd.DataFrame) -> List[CounterpartyModel]:
    """
    Преобразует DataFrame в список Pydantic моделей с обработкой ошибок типов.

    Args:
        df: DataFrame с данными из Google Sheets

    Returns:
        Список моделей CounterpartyModel
    """
    models = []

    # Сначала предобработаем DataFrame
    df_clean = df.copy()

    # Заменяем пустые строки и NaN на None
    df_clean = df_clean.replace(['', ' ', '  ', pd.NA, pd.NaT], None)

    # Обрабатываем колонку с ID - преобразуем float в int
    if '№' in df_clean.columns:
        df_clean['№'] = df_clean['№'].apply(
            lambda x: int(x) if pd.notnull(x) and str(x).strip() != '' else None
        )

    # Обрабатываем числовые поля
    numeric_fields = ['№']  # добавь другие числовые поля если есть

    for field in numeric_fields:
        if field in df_clean.columns:
            df_clean[field] = pd.to_numeric(df_clean[field], errors='coerce')

    # Обрабатываем даты
    date_columns = ['Дата обновления информации по благонадежности']
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', dayfirst=True).dt.date

    # Преобразуем каждую строку
    for index, row in df_clean.iterrows():
        try:
            # Преобразуем строку в словарь, заменяя оставшиеся NaN на None
            row_dict = {}
            for col, value in row.items():
                if pd.isna(value):
                    row_dict[col] = None
                elif isinstance(value, float) and pd.notnull(value):
                    # Для float, которые могут быть int
                    if col == '№':
                        row_dict[col] = int(value) if value == int(value) else value
                    else:
                        row_dict[col] = value
                else:
                    row_dict[col] = value

            # Пропускаем строки без ID (они не имеют смысла)
            if row_dict.get('№') is None:
                print(f"Пропущена строка {index + 2}: нет ID")
                continue

            # Создаем модель
            model = CounterpartyModel(**row_dict)
            models.append(model)

        except Exception as e:
            # Более детальная информация об ошибке
            row_id = row.get('№', f'строка_{index + 2}')
            print(f"Ошибка в строке {row_id}: {type(e).__name__}: {e}")
            print(f"Данные строки: {row.to_dict()}")
            continue

    print(f"Успешно создано: {len(models)} моделей из {len(df)} строк")
    return models