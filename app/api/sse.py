import json
from typing import Any


def encode_sse(payload: dict[str, Any]) -> str:
    """将一个应用消息编码为一条 data-only SSE 事件。"""
    if not isinstance(payload, dict):
        raise TypeError("本项目的 SSE 数据必须是 JSON 对象")
    # JSON 转义内容中的换行，避免它们被当成 SSE 的事件边界。
    data = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    return f"data: {data}\n\n"
