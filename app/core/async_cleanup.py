import asyncio
from collections.abc import Awaitable

import anyio

from app.core.log import logger


async def complete_cleanup(cleanup: Awaitable):
    """等待短期异步收尾完成，再继续传播调用者收到的取消。"""
    future = asyncio.ensure_future(cleanup)
    cancelled = False
    with anyio.CancelScope(shield=True):
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        try:
            result = future.result()
        except Exception as error:
            if cancelled:
                logger.error("取消期间的资源清理失败，异常类型：{}", type(error).__name__)
                raise asyncio.CancelledError() from error
            raise
    if cancelled:
        raise asyncio.CancelledError()
    return result
