from fastapi import APIRouter, Depends

from app.api.dependencies import get_query_service
from app.api.schemas.error_schema import ErrorSchema
from app.api.schemas.query_schema import QuerySchema
from app.api.streaming_response import QueryStreamingResponse
from app.services.query_service import QueryService


query_router = APIRouter(tags=["问数"])


@query_router.post(
    "/api/query",
    summary="自然语言问数",
    response_class=QueryStreamingResponse,
    responses={
        200: {"content": {"text/event-stream": {}}},
        422: {"model": ErrorSchema, "description": "请求参数不合法"},
        503: {"model": ErrorSchema, "description": "查询依赖尚未就绪"},
        500: {"model": ErrorSchema, "description": "响应开始前发生服务异常"},
    },
)
async def query(
    payload: QuerySchema,
    query_service: QueryService = Depends(get_query_service),
) -> QueryStreamingResponse:
    return QueryStreamingResponse(
        query_service.query(payload.query),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )
