from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import Depends, HTTPException, status
from langchain_huggingface import HuggingFaceEndpointEmbeddings
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.mysql_client_manager import dw_mysql_client_manager, meta_mysql_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager
from app.core.async_cleanup import complete_cleanup
from app.repositories.es.value_es_repository import ValueEsRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository
from app.services.query_service import QueryService


def _require_initialized(resource):
    """只检查资源已提供；实际连接健康检查由后续集成处理。"""
    if resource is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="查询服务尚未就绪，请稍后重试。",
        )
    return resource


@asynccontextmanager
async def _request_session(session_factory):
    async with AsyncExitStack() as stack:
        session = await stack.enter_async_context(session_factory())
        try:
            yield session
        finally:
            # 请求/查询仍可取消，只保护会话退出时的异步清理。
            await complete_cleanup(stack.aclose())


async def get_meta_session() -> AsyncIterator[AsyncSession]:
    session_factory = _require_initialized(meta_mysql_client_manager.session_factory)
    async with _request_session(session_factory) as session:
        yield session


async def get_dw_session() -> AsyncIterator[AsyncSession]:
    session_factory = _require_initialized(dw_mysql_client_manager.session_factory)
    async with _request_session(session_factory) as session:
        yield session


async def get_embedding_client() -> HuggingFaceEndpointEmbeddings:
    return _require_initialized(embedding_client_manager.client)


async def get_column_qdrant_repository() -> ColumnQdrantRepository:
    return ColumnQdrantRepository(_require_initialized(qdrant_client_manager.client))


async def get_value_es_repository() -> ValueEsRepository:
    return ValueEsRepository(_require_initialized(es_client_manager.client))


async def get_metric_qdrant_repository() -> MetricQdrantRepository:
    return MetricQdrantRepository(_require_initialized(qdrant_client_manager.client))


async def get_meta_mysql_repository(
    session: AsyncSession = Depends(get_meta_session, scope="request"),
) -> MetaMySQLRepository:
    return MetaMySQLRepository(session)


async def get_dw_mysql_repository(
    session: AsyncSession = Depends(get_dw_session, scope="request"),
) -> DWMySQLRepository:
    return DWMySQLRepository(session)


async def get_query_service(
    embedding_client: HuggingFaceEndpointEmbeddings = Depends(get_embedding_client),
    column_qdrant_repository: ColumnQdrantRepository = Depends(get_column_qdrant_repository),
    value_es_repository: ValueEsRepository = Depends(get_value_es_repository),
    metric_qdrant_repository: MetricQdrantRepository = Depends(get_metric_qdrant_repository),
    meta_mysql_repository: MetaMySQLRepository = Depends(get_meta_mysql_repository),
    dw_mysql_repository: DWMySQLRepository = Depends(get_dw_mysql_repository),
) -> QueryService:
    return QueryService(
        embedding_client=embedding_client,
        column_qdrant_repository=column_qdrant_repository,
        value_es_repository=value_es_repository,
        metric_qdrant_repository=metric_qdrant_repository,
        meta_mysql_repository=meta_mysql_repository,
        dw_mysql_repository=dw_mysql_repository,
    )
