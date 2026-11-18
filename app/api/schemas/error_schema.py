from typing import Literal

from pydantic import BaseModel


class ValidationErrorDetail(BaseModel):
    location: list[str | int]
    type: str
    message: str


class ErrorSchema(BaseModel):
    """响应开始前的 JSON 错误；不包含原始请求或内部异常文本。"""

    type: Literal["error"] = "error"
    message: str
    details: list[ValidationErrorDetail] | None = None
