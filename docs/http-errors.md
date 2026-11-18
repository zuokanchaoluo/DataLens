API 第 10 模块：流式响应开始前的异常处理

本模块为正式 FastAPI 应用增加 HTTP 异常处理器，统一 JSON 错误格式，覆盖参数错误、依赖未就绪和响应开始前的服务异常。SSE error 继续由 QueryService 输出；第 11 模块已补齐流内终止与断开连接，说明见 `docs/streaming-errors.md`。

原开发文档给出了 QueryService 内部的基础 try/except 流式错误示例，没有独立的全局 HTTP 异常处理章节。本模块依据项目已经确定的 API 学习计划补充，不把参考文档中的基础流内处理误用到响应开始前。

**改动位置**

| 文件 | 职责 |
| --- | --- |
| `app/api/schemas/error_schema.py` | ErrorSchema 与参数错误明细结构 |
| `app/api/exception_handlers.py` | 三类全局异常处理器及注册函数 |
| `main.py` | 在正式应用注册异常处理器 |
| `app/api/routers/query_router.py` | OpenAPI 声明 422、500、503 JSON 错误 |
| `tests/test_http_errors.py` | 13 项状态码、内容筛选、资源清理和响应边界验证 |

**先判断响应是否已经开始**

HTTP 状态码和响应头在响应开始时发送。对应 ASGI 的 `http.response.start`，并不是“客户端看见第一条 SSE”这一时刻。

| 发生位置 | 当前行为 |
| --- | --- |
| 请求体校验失败 | HTTP 422，application/json |
| 依赖对象尚未初始化 | HTTP 503，application/json |
| 构造仓库/QueryService、调用服务取得生成器时出现未知异常 | HTTP 500，application/json |
| StreamingResponse 已开始发送，工作流随后失败 | 通常已是 HTTP 200，由 QueryService 输出 SSE error |
| 应用 lifespan 启动失败 | 启动失败并清理资源，此时没有可返回 JSON 的 HTTP 请求 |

正常的 QueryService.query 是异步生成器，调用它只得到生成器对象，函数体在消费时才执行。StreamingResponse 会先开始 HTTP 响应，再消费生成器。因此，即使工作流在第一条进度之前就报错，也不能据此认定错误发生在 HTTP 响应开始前。本轮有专门测试验证该情况仍得到 200 + SSE error。

**错误体的约定**

所有本模块处理的 HTTP 错误使用 application/json，基本结构为：

```json
{"type":"error","message":"服务暂时无法完成请求，请稍后重试。"}
```

参数校验错误额外带 details。例如缺少 query 时：

```json
{
  "type": "error",
  "message": "请求参数不合法，请检查提交内容。",
  "details": [
    {
      "location": ["body", "query"],
      "type": "missing",
      "message": "Field required"
    }
  ]
}
```

location 表示请求中的错误位置，type 是校验类型，message 是框架提供的校验说明。服务显式选择这三个字段，不把完整的 `error.errors()`、其中的 input/ctx、原始请求体直接返回或写入新增日志。

ErrorSchema 的 details 是可选字段，非参数错误通过 model_dump(exclude_none=True) 省略它。HTTP 错误与 SSE error 都使用 type/message，客户端仍需根据 HTTP 状态和 Content-Type 选择 JSON 或 SSE 解析方式。

第 8 模块中依赖未就绪时的默认 `{"detail": ...}`，已改为：

```json
{"type":"error","message":"查询服务尚未就绪，请稍后重试。"}
```

现有前端的 response.ok 和错误展示将在后续前后端联调时处理，本模块没有改动前端。

**为什么需要三个处理器**

main.py 调用 register_exception_handlers(app)，内部注册：

```python
app.add_exception_handler(RequestValidationError, request_validation_error_handler)
app.add_exception_handler(HTTPException, http_error_handler)
app.add_exception_handler(Exception, unexpected_error_handler)
```

RequestValidationError 是 FastAPI 对请求校验失败的异常，不代表任意 Python ValueError。仓库或服务代码抛出的 ValueError 仍可能是程序内部问题，应返回 500，不能全部归为用户输入错误。

HTTPException 处理器注册 Starlette 的基类，既覆盖 FastAPI 主动抛出的 HTTPException，也覆盖路由框架生成的 404、405。它保留原状态码与 headers，因此 405 的 Allow、401 的 WWW-Authenticate、503 的 Retry-After 等响应头仍然可以使用。本项目没有因此新增登录或限流功能，这些头只用于验证通用处理器的行为。

主动声明的 4xx 字符串说明按约定返回；detail 为复杂对象时使用通用说明。5xx 不直接使用原始 detail：503 返回“查询服务尚未就绪”，其他 5xx 返回通用内部错误消息。未知 Exception 统一返回 500，并在新增应用日志中只记录异常类型。

异常处理器是框架识别到异常后调用的函数，不需要在每个依赖和路由里重复 try/except。[FastAPI 官方异常处理说明](https://fastapi.tiangolo.com/tutorial/handling-errors/)介绍了覆盖默认处理器、RequestValidationError 和 Starlette/FastAPI HTTPException 的关系。

**异常处理与资源清理分别负责什么**

全局处理器决定客户端收到的状态码和 JSON 内容。Session 仍由 yield 依赖的 async with 清理，共享客户端仍由 lifespan 管理，不在处理器中重复关闭。

本轮验证：dw 会话工厂失败时，已创建的 meta 会话会关闭；服务构造失败时，已创建的两份会话会关闭；共享客户端保持开启，后续正常请求仍可运行。响应开始前的任务取消继续向上传播，不转换为普通 500。

这不是一次真实数据库连接健康检查；例如 SQL 查询中首次连接数据库时失败，通常已经进入流式阶段，会走现有 SSE 错误路径。

**OpenAPI 与验证方法**

`POST /api/query` 的 200 响应仍声明 text/event-stream；422、500、503 则引用 ErrorSchema，类型为 application/json。访问 `http://127.0.0.1:8000/docs` 可以查看。

在项目根目录、激活 Python 3.12 虚拟环境后：

```powershell
python -m unittest discover -s tests -p test_http_errors.py -v
```

测试使用正式应用、正式 lifespan、真实管理器、替身 SDK 与工作流。对未知 500，Starlette 发送错误响应后仍会向服务器/测试客户端重新抛出异常，因此测试使用 `httpx.ASGITransport(..., raise_app_exceptions=False)` 来检查客户端实际得到的 JSON；这不是业务代码吞掉异常。

2026-11-18，新增 13 项测试通过，全量离线回归 241 项通过，耗时 19.884 秒。覆盖参数与非法 JSON、依赖缺失、会话工厂/仓库/服务故障、必要响应头、404/405、取消、OpenAPI，以及第一条工作流事件前失败仍使用 SSE 的边界。

本机真实 Uvicorn 验证同样通过，报告 `.local/http_errors_check.json`，脚本 `.local/http_errors_check.py`，临时服务已停止。验证未调用真实模型、业务数据库或外部存储。第 11 模块现已完成，当前进度见 API 开发说明。
