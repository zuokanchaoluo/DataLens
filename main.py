from fastapi import FastAPI

from app.api.exception_handlers import register_exception_handlers
from app.api.routers.query_router import query_router
from app.core.lifespan import lifespan
from app.core.request_id import RequestIdMiddleware


app = FastAPI(
    title="数析 DataLens API",
    description="接收自然语言问题，提供问数进度和查询结果。",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(RequestIdMiddleware)
register_exception_handlers(app)
app.include_router(query_router)
