"""流式响应学习示例：uvicorn app.scripts.streaming_demo:app --port 8001。"""
import asyncio
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse

from app.api.sse import encode_sse

app = FastAPI(title="流式响应学习示例")


async def generate_messages() -> AsyncIterator[str]:
    """第一条立即产生，后续每隔一秒产生一条，共五条。"""
    for index in range(1, 6):
        if index > 1:
            await asyncio.sleep(1)
        yield f"第 {index} 条示例消息\n"


@app.get("/demo/normal", response_class=PlainTextResponse)
async def normal_response() -> PlainTextResponse:
    parts = []
    async for message in generate_messages():
        parts.append(message)
    return PlainTextResponse("".join(parts))


@app.get(
    "/demo/stream",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/plain": {}}}},
)
async def streaming_response() -> StreamingResponse:
    return StreamingResponse(generate_messages(), media_type="text/plain")


async def generate_sse_events(fail: bool = False) -> AsyncIterator[str]:
    yield encode_sse({"type": "progress", "step": "SSE消息演示", "status": "running"})
    await asyncio.sleep(1)
    if fail:
        yield encode_sse({"type": "progress", "step": "SSE消息演示", "status": "error"})
        yield encode_sse({"type": "error", "message": "这是一条演示错误消息。"})
        return
    yield encode_sse({"type": "progress", "step": "SSE消息演示", "status": "success"})
    yield encode_sse({
        "type": "result", "data": [{"示例字段": "你好，SSE！"}],
        "columns": ["示例字段"], "row_count": 1, "truncated": False,
    })


@app.get(
    "/demo/sse",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}},
)
async def sse_response(fail: bool = False) -> StreamingResponse:
    return StreamingResponse(
        generate_sse_events(fail), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )
