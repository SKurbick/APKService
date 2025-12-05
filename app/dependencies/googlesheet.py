from asyncpg import Pool
from fastapi import Depends
from starlette.requests import Request

from app.service.googlesheet import GoogleSheetService
from app.database.repositories.googlesheet import GoogleSheetRepository


def get_pool(request: Request) -> Pool:
    """Получение пула соединений из состояния приложения."""
    return request.app.state.pool


def get_googlesheet_repository(pool: Pool = Depends(get_pool)) -> GoogleSheetRepository:
    return GoogleSheetRepository(pool)


def get_googlesheet_service(repository: GoogleSheetRepository = Depends(get_googlesheet_repository)) -> GoogleSheetService:
    return GoogleSheetService(repository)
