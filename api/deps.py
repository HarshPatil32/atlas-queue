from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from core.db import get_session


async def get_db() -> AsyncIterator[AsyncSession]:
    async with get_session() as session:
        yield session
