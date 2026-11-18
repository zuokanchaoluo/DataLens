"""中间件学习示例：uvicorn app.scripts.middleware_demo:app --port 8003。"""
import asyncio
from time import perf_counter

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from app.api.sse import encode_sse


app = FastAPI(title="中间件学习示例")


@app.middleware("http")
async def observe_request(request: Request, call_next):
    started = perf_counter()
    request.state.steps = ["middleware.before"]
    print(f"中间件前：{request.url.path}", flush=True)

    response = await call_next(request)

    request.state.steps.append("middleware.after")
    response.headers["X-Process-Time"] = f"{perf_counter() - started:.6f}"
    response.headers["X-Demo-Trace"] = " -> ".join(request.state.steps)
    print(f"中间件后：{request.url.path}，响应已就绪。", flush=True)
    return response


@app.get("/demo/middleware")
async def inspect_request(request: Request, label: str = "demo"):
    request.state.steps.append("handler")
    request.state.label = label
    print("接口：处理演示请求。", flush=True)
    # 模拟异步等待，使并发请求有机会交错执行。
    await asyncio.sleep(0.01)
    return {"label": request.state.label, "steps": list(request.state.steps)}


async def generate_stream():
    yield encode_sse({"type": "progress", "step": "中间件流式演示", "status": "running"})
    await asyncio.sleep(1)
    yield encode_sse({"type": "result", "data": [{"示例": "流发送完毕"}]})


@app.get(
    "/demo/middleware/stream",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}},
)
async def inspect_stream(request: Request) -> StreamingResponse:
    request.state.steps.append("handler")
    print("接口：返回流式响应对象。", flush=True)
    return StreamingResponse(generate_stream(), media_type="text/event-stream")
