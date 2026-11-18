from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.mysql_client_manager import dw_mysql_client_manager, meta_mysql_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 全局管理器只能由一个运行中的生命周期持有，不能覆盖其他调用者的资源。
    existing_resources = (
        embedding_client_manager.client, qdrant_client_manager.client, es_client_manager.client,
        meta_mysql_client_manager.engine, meta_mysql_client_manager.session_factory,
        dw_mysql_client_manager.engine, dw_mysql_client_manager.session_factory,
    )
    if any(resource is not None for resource in existing_resources):
        raise RuntimeError("共享资源已初始化，请先关闭后再启动应用生命周期")

    async with AsyncExitStack() as stack:
        for manager in (
            embedding_client_manager, qdrant_client_manager, es_client_manager,
            meta_mysql_client_manager, dw_mysql_client_manager,
        ):
            # 先登记，覆盖 init 内部创建了一部分资源后才抛出异常的情况。
            stack.push_async_callback(manager.close)
            manager.init()
        yield
