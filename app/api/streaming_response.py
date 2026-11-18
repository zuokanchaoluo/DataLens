import asyncio
from contextlib import aclosing

from fastapi.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from app.core.async_cleanup import complete_cleanup
from app.core.log import logger


class QueryStreamingResponse(StreamingResponse):
    """本项目异步生成器的响应：监听断开，并等待生成器关闭后才结束请求。"""

    async def _send_and_close(self, send: Send) -> None:
        # 与消费生成器在同一个任务中关闭，覆盖停在 yield 时发送失败的情况。
        async with aclosing(self.body_iterator):
            await self.stream_response(send)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # 使用 asyncio 任务执行工作流，避免 AnyIO 取消域反复打断节点的异步清理。
        stream_task = asyncio.create_task(self._send_and_close(send))
        disconnect_task = asyncio.create_task(self.listen_for_disconnect(receive))
        tasks = (stream_task, disconnect_task)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if stream_task in done:
                await stream_task
            else:
                await disconnect_task
        except OSError:
            # ASGI send 在连接关闭时可抛出 OSError；不再向断开的连接发送错误。
            pass
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            # 只保护收尾，正常模型/数据库等待仍然可以取消。
            async def close_tasks_and_iterator():
                outcomes = await asyncio.gather(*tasks, return_exceptions=True)
                # 若发送任务尚未开始就被取消，生成器也应显式关闭。
                await self.body_iterator.aclose()
                return outcomes

            outcomes = await complete_cleanup(close_tasks_and_iterator())
            for outcome in outcomes:
                if isinstance(outcome, Exception) and not isinstance(outcome, OSError):
                    logger.error("SSE 收尾任务异常，异常类型：{}", type(outcome).__name__)

        if self.background is not None:
            await self.background()
