from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from app.api.schemas.error_schema import ErrorSchema, ValidationErrorDetail
from app.core.context import REQUEST_ID_HEADER, request_id_ctx_var
from app.core.log import logger


VALIDATION_ERROR_MESSAGE = "请求参数不合法，请检查提交内容。"
SERVICE_UNAVAILABLE_MESSAGE = "查询服务尚未就绪，请稍后重试。"
INTERNAL_ERROR_MESSAGE = "服务暂时无法完成请求，请稍后重试。"


async def request_validation_error_handler(request: Request, error: RequestValidationError) -> JSONResponse:
    details = [
        ValidationErrorDetail(location=list(item["loc"]), type=item["type"], message=item["msg"])
        for item in error.errors()
    ]
    # 显式选择字段，避免回传 input、ctx 或整个请求体。
    payload = ErrorSchema(message=VALIDATION_ERROR_MESSAGE, details=details)
    return JSONResponse(status_code=422, content=payload.model_dump(exclude_none=True))


async def http_error_handler(request: Request, error: HTTPException) -> JSONResponse:
    if error.status_code >= 500:
        message = SERVICE_UNAVAILABLE_MESSAGE if error.status_code == 503 else INTERNAL_ERROR_MESSAGE
        logger.warning("HTTP 请求未完成，状态码：{}", error.status_code)
    else:
        message = error.detail if isinstance(error.detail, str) else "请求未能完成，请检查请求。"
    payload = ErrorSchema(message=message)
    return JSONResponse(
        status_code=error.status_code, content=payload.model_dump(exclude_none=True), headers=error.headers,
    )


async def unexpected_error_handler(request: Request, error: Exception) -> JSONResponse:
    # ServerErrorMiddleware 位于用户中间件外层，此时请求上下文可能已经重置。
    request_id = getattr(request.state, "request_id", None)
    token = request_id_ctx_var.set(request_id) if request_id is not None else None
    try:
        # 未知错误不能当成客户端参数错误，也不直接暴露 str(error)。
        logger.error("HTTP 请求发生未处理异常，异常类型：{}", type(error).__name__)
        payload = ErrorSchema(message=INTERNAL_ERROR_MESSAGE)
        headers = {REQUEST_ID_HEADER: request_id} if request_id is not None else None
        return JSONResponse(status_code=500, content=payload.model_dump(exclude_none=True), headers=headers)
    finally:
        if token is not None:
            request_id_ctx_var.reset(token)


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    # Starlette 基类同时覆盖框架的 404/405 与 FastAPI HTTPException。
    app.add_exception_handler(HTTPException, http_error_handler)
    app.add_exception_handler(Exception, unexpected_error_handler)
