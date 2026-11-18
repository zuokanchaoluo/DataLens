from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.context import REQUEST_ID_HEADER, request_id_ctx_var


class RequestIdMiddleware:
    """为每个 HTTP 请求生成标识，覆盖整个响应和请求资源清理。"""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = uuid4().hex
        token = request_id_ctx_var.set(request_id)
        try:
            # 保存到请求自身，供位于用户中间件外层的 500 处理器取回。
            scope.setdefault("state", {})["request_id"] = request_id

            async def send_with_request_id(message):
                if message["type"] == "http.response.start":
                    MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
                await send(message)

            await self.app(scope, receive, send_with_request_id)
        finally:
            request_id_ctx_var.reset(token)
