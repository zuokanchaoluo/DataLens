"""依赖项学习示例：uvicorn app.scripts.dependencies_demo:app --port 8004。"""
import asyncio

import httpx
from fastapi import Depends, FastAPI
from fastapi.responses import StreamingResponse

from app.api.schemas.query_schema import QuerySchema
from app.api.sse import encode_sse


app = FastAPI(title="依赖项学习示例")


async def get_demo_client():
    """使用可关闭对象模拟请求级资源，不向外发送请求。"""
    client = httpx.AsyncClient(timeout=5, trust_env=False)
    try:
        print("依赖进入：本次请求资源已创建。", flush=True)
        yield client
    finally:
        await client.aclose()
        print("依赖退出：本次请求资源已关闭。", flush=True)


async def get_nested_client(
    client: httpx.AsyncClient = Depends(get_demo_client, scope="request"),
) -> httpx.AsyncClient:
    print("子依赖：取得本次请求资源。", flush=True)
    return client


@app.post("/demo/dependencies")
async def inspect_dependencies(
    payload: QuerySchema,
    client: httpx.AsyncClient = Depends(get_demo_client, scope="request"),
    nested_client: httpx.AsyncClient = Depends(get_nested_client),
):
    print("接口：使用已经注入的对象。", flush=True)
    await asyncio.sleep(0.01)
    return {"query": payload.query, "resource_open": not client.is_closed,
            "same_client": client is nested_client}


async def generate_stream(client: httpx.AsyncClient):
    yield encode_sse({"type": "progress", "step": "依赖资源演示", "status": "running",
                      "resource_open": not client.is_closed})
    await asyncio.sleep(1)
    yield encode_sse({"type": "result", "data": [{"resource_open": not client.is_closed}],
                      "columns": ["resource_open"], "row_count": 1, "truncated": False})


@app.get(
    "/demo/dependencies/stream",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}},
)
async def inspect_stream(
    client: httpx.AsyncClient = Depends(get_demo_client, scope="request"),
) -> StreamingResponse:
    print("接口：返回使用本次请求资源的流。", flush=True)
    return StreamingResponse(generate_stream(client), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})
