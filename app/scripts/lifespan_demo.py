"""生命周期学习示例：uvicorn app.scripts.lifespan_demo:app --port 8002。"""
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = httpx.AsyncClient(timeout=5, trust_env=False)
    try:
        app.state.demo_client = client
        app.state.request_count = 0
        print("启动：演示 HTTP 客户端已创建，准备接收请求。", flush=True)
        yield
    finally:
        await client.aclose()
        print("关闭：演示 HTTP 客户端已释放。", flush=True)


app = FastAPI(title="生命周期学习示例", lifespan=lifespan)


@app.get("/demo/lifespan")
async def resource_status(request: Request):
    client = request.app.state.demo_client
    request.app.state.request_count += 1
    count = request.app.state.request_count
    print(f"请求：第 {count} 次访问，客户端开启={not client.is_closed}。", flush=True)
    return {"resource_open": not client.is_closed, "request_count": count}
