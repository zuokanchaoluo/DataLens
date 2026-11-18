掌柜问数 API 开发说明

**当前进度：第 1～12 个 API 小模块均已完成**

已实现 FastAPI 入口、请求模型、QueryService 和正式 `POST /api/query` 路由，依赖项会按请求构造会话、仓库与服务。共享资源由应用生命周期管理，HTTP/SSE 异常与 request_id 已接通。2026-11-18 正式 API 使用真实模型及全部外部依赖完成 3/3 HTTP 问数验证，结果与独立只读 SQL 一致；详见 `docs/query-api-integration.md`。

前端第 1 小模块也已完成：HTTP 错误检查、中文提示及请求编号。8 项前端测试、构建和浏览器 8 种模拟场景通过，详见 [前端 HTTP 错误提示](frontend-http-errors.md)。

前端第 2 小模块已完成空结果展示：直接使用 result.columns，零行时保留列头并显示“查询成功，暂无数据”。既有 8 项前端测试、构建与本模块浏览器 8 种模拟场景通过，详见 [前端空结果展示](frontend-empty-results.md)。

第 2 模块新增独立教学应用 `app/scripts/streaming_demo.py`，通过普通响应和流式响应对比消息到达时间。使用 `python -m uvicorn app.scripts.streaming_demo:app --port 8001` 启动，访问 `/demo/normal` 和 `/demo/stream`；操作与原理见 `docs/streaming-response.md`。

第 3 模块实现 `app/api/sse.py` 中的 SSE JSON 编码工具，并在教学应用增加 `/demo/sse` 和 `/demo/sse?fail=true`，演示进度、结果和错误消息。协议、边界和测试说明见 `docs/sse.md`。

第 4 模块新增独立生命周期教学应用 `app/scripts/lifespan_demo.py`，使用真实但不发送请求的 HTTP 客户端演示启动创建、请求复用、关闭释放。用 `python -m uvicorn app.scripts.lifespan_demo:app --port 8002` 启动，访问 `/demo/lifespan`；说明见 `docs/lifespan.md`。正式应用的外部客户端管理已在第 9 模块接入。

第 5 模块新增中间件教学应用 `app/scripts/middleware_demo.py`，演示请求前后执行顺序、响应头、request.state 隔离和流式响应的计时边界。用 `python -m uvicorn app.scripts.middleware_demo:app --port 8003` 启动，说明见 `docs/middleware.md`。正式 request_id 日志已在第 12 模块接入。

第 6 模块新增依赖项教学应用 `app/scripts/dependencies_demo.py`，演示 Depends、子依赖、请求内缓存和带 yield 的请求资源清理，并验证流式响应结束前资源保持可用。用 `python -m uvicorn app.scripts.dependencies_demo:app --port 8004` 启动，说明见 `docs/dependencies.md`。

第 7 模块新增 `app/services/query_service.py`，将现有工作流转换为 SSE 异步事件流，兼容旧字符串进度，检查最终状态并输出结果或通用终止错误。

第 8 模块新增 `app/api/dependencies.py` 与 `app/api/routers/query_router.py`，在 `main.py` 注册正式路由。MySQL 会话通过请求作用域的 yield 依赖保持到流式响应结束，仓库和 QueryService 在请求内构造。详细原理和验证见 `docs/query-dependencies.md`。

第 9 模块新增 `app/core/lifespan.py` 并在 `main.py` 注册，通过 AsyncExitStack 管理五个共享管理器。四类管理器补齐重复初始化检查、重复关闭和引用清理；Embedding 的同步与异步客户端都会关闭。详细说明见 `docs/app-lifespan.md`。

第 10 模块新增全局 HTTP 异常处理和 ErrorSchema，统一响应开始前的 JSON 错误：参数错误 422、依赖未就绪 503、未知内部异常 500。保留参数错误位置，隐藏原始请求值与内部故障文本，OpenAPI 已声明错误响应。详细说明见 `docs/http-errors.md`。

第 11 模块新增 QueryStreamingResponse 与 complete_cleanup，监听客户端断开，取消并等待发送/工作流任务，关闭生成器后再释放请求会话。QueryService 在工作流清理之后最多生成一条终止事件，取消或断开不追加业务错误。详细说明见 `docs/streaming-errors.md`。

第 12 模块新增 ContextVar 和纯 ASGI 请求中间件，每个 HTTP 请求由服务器生成独立的 UUID hex 标识，响应头使用 X-Request-ID，项目 Loguru 日志自动附带同一标识。上下文覆盖完整 SSE 与会话清理，finally 重置 token；未知 500 处理器从 request.state 取回 ID。详细说明见 `docs/request-id.md`。

启动方式（在项目根目录，激活 Python 3.12 虚拟环境并准备配置中的百炼密钥后）：

```powershell
if (-not $env:OPENAI_API_KEY) {
    $env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')
}
python -m uvicorn main:app --reload --port 8000
```

访问 `http://127.0.0.1:8000/docs` 可以看到并提交 `POST /api/query`。导入服务会加载现有模型配置并构造模型对象，因此需要有效的 `OPENAI_API_KEY` 环境变量；启动时 lifespan 初始化外部客户端与连接池，实际查询还需要 MySQL、Qdrant、ES、Embedding、元数据和知识索引可用。初始化对象不等于完成全部外部服务健康检查；资源初始化缺失时的 503 保护仍保留。

**接口需求**

已有智能体能够接收一个问题、召回上下文、生成并校验 SQL、执行查询。API 层负责把这一能力提供给浏览器等客户端，并实时传递执行进度。

目标接口：

| 项目 | 约定 |
| --- | --- |
| 方法与路径 | `POST /api/query` |
| 请求格式 | `Content-Type: application/json` |
| 请求体 | `{"query": "统计华北地区的销售总额"}` |
| 成功响应类型 | `Content-Type: text/event-stream` |
| 响应内容 | 依次到达的进度、查询结果或终止错误事件 |

接口路径和请求格式与开发文档一致，也与现有 Vue 前端的 `fetch('/api/query', ...)` 一致。前端开发代理已将 `/api` 转发至 `http://localhost:8000`。

**请求模型已经实现**

文件：`app/api/schemas/query_schema.py`。

- `query` 必须提供，且必须是字符串。
- 去除首尾空白后，问题不能为空；保留问题内部的空格和换行。
- 未声明的额外字段会被拒绝，客户端只提交自然语言问题。

文档中的基础版本只声明 `query: str`；本项目在此基础上增加了上述输入校验。`QuerySchema` 只描述 HTTP 输入；工作流使用的 `DataAgentState` 还包含召回、SQL、错误和结果等内部数据。

正式路由已绑定该模型。依赖可用时，非法请求返回 HTTP 422，不进入智能体；第 10 模块已将错误体统一为 ErrorSchema，details 只保留错误位置、类型和说明。FastAPI 可能先解析依赖再报告请求体错误，已创建的会话也会关闭；资源未就绪时可能先返回 503。

响应开始前的错误使用 application/json，例如依赖未就绪：

```json
{"type":"error","message":"查询服务尚未就绪，请稍后重试。"}
```

这取代第 8 模块中默认的 `{"detail": ...}` 格式。参数校验错误还包含 details 数组，具体契约见 `docs/http-errors.md`。响应开始后仍沿用下面的 SSE 事件协议。

第 12 模块为正式应用的 HTTP 响应增加 X-Request-ID，包括 SSE、422/503/500、404/405 和文档页。标识为服务器新生成的 32 位十六进制字符串，客户端提交的同名头不会复用。JSON/SSE 数据结构保留，request_id 通过响应头提供。

**事件契约**

传输层使用 SSE，应用数据放在每个事件的 JSON 中。编码工具和教学接口已经实现，客户端拆包和 UTF-8 增量解码已通过测试；正式路由通过继承自 StreamingResponse 的 QueryStreamingResponse 消费 QueryService 生成的 SSE 文本。

进度事件示例：

```json
{"type":"progress","step":"生成SQL","status":"running"}
```

`status` 使用 `running`、`success` 或 `error`。某个校验步骤出错后可能进入纠错，因此进度中的 `error` 不一定意味着整个流程结束。

结果事件沿用已有执行节点的数据结构：

```json
{
  "type": "result",
  "data": [{"total_amount": "41099.50"}],
  "columns": ["total_amount"],
  "row_count": 1,
  "truncated": false
}
```

空结果使用 `type=result`、`data=[]`、`row_count=0` 并保留列头。截断时表格上方使用 row_count 显示本次返回的行数。

示例金额是字符串，因为已有 SQL 结果转换器会把 Decimal 转成字符串以保留精度；QueryService 沿用节点输出，不再次改变数值类型。

终止错误事件的目标结构：

```json
{"type":"error","message":"本次查询未能完成，请稍后重试。"}
```

QueryService 已对 SQL 纠错次数耗尽、执行异常等情况输出上述通用终止错误，不将原始错误细节发送给客户端。第 10 模块处理响应开始前的 HTTP 错误，第 11 模块已验证流内终止、取消/断开与资源释放。保持连接并正常消费时，终止事件是 result 或 error 中的一条；取消、断开或传输失败时不承诺客户端收到终止事件。

**代码职责规划**

| 文件 | 职责与状态 |
| --- | --- |
| `main.py` | 已注册正式查询路由、lifespan、HTTP 异常处理与 request_id 中间件 |
| `app/api/schemas/query_schema.py` | 请求体校验，已实现 |
| `app/api/schemas/error_schema.py` | 已实现：JSON 错误体和参数错误明细结构 |
| `app/api/exception_handlers.py` | 已实现：注册参数校验、HTTPException 和未知异常的处理器 |
| `app/api/streaming_response.py` | 已实现：监听断开，等待发送任务与异步生成器关闭 |
| `app/core/async_cleanup.py` | 已实现：保护异步收尾，等待完成后继续传播取消 |
| `app/scripts/streaming_demo.py` | 已实现：独立的普通响应/流式响应教学应用 |
| `app/scripts/lifespan_demo.py` | 已实现：独立的启动、资源复用和关闭教学应用 |
| `app/scripts/middleware_demo.py` | 已实现：独立的中间件顺序与请求隔离教学应用 |
| `app/scripts/dependencies_demo.py` | 已实现：独立的依赖解析、请求内复用和清理教学应用 |
| `app/api/sse.py` | 已实现：将 JSON 对象编码为单条 SSE 事件 |
| `app/api/routers/query_router.py` | 已实现：接收请求并返回 SSE 流式响应 |
| `app/services/query_service.py` | 已实现：调用工作流、适配进度、检查终态并生成 SSE 文本 |
| `app/api/dependencies.py` | 已实现：提供会话、仓库和 QueryService，检查资源是否已初始化 |
| `app/core/lifespan.py` | 已实现：启动初始化共享客户端/连接池，逆序关闭，启动失败也清理 |
| `app/core/context.py` | 已实现：ContextVar 保存当前 request_id，非 HTTP 默认值为 - |
| `app/core/request_id.py` | 已实现：纯 ASGI 中间件，生成 ID、写入请求状态/响应头，finally 重置 |
| `app/core/log.py` | 已实现：保留日志配置，每条日志动态注入 request_id |

现有 `graph.create_context()` 用于命令行单次运行，会初始化和关闭全局客户端，正式路由不调用它。第 8 模块已将数据库会话、仓库、QueryService 和 State 按请求隔离，并让会话保持到流式响应结束；客户端和连接池由第 9 模块的应用生命周期管理。

**第 7 模块：QueryService 的具体运行过程**

QueryService 是服务层：接收问题，使用已经构造好的依赖运行智能体，把内部输出转换为接口能够消费的消息。`graph.py` 决定节点如何执行，`encode_sse()` 决定单条事件怎样编码，QueryService 将这两部分连接起来。

可以直接用 Python 消费这个服务的异步生成器。第 8 模块也已接入正式调用链：请求模型与依赖解析 → 路由取得 QueryService → `StreamingResponse` 消费 `query()` → 工作流运行 → 客户端收到 SSE。第 9 模块已让共享资源随应用启动和关闭。

构造函数接收六类对象：

| 参数 | 在工作流中的用途 |
| --- | --- |
| `embedding_client` | 把关键词转成向量 |
| `column_qdrant_repository` | 召回字段元数据 |
| `metric_qdrant_repository` | 召回指标元数据 |
| `value_es_repository` | 召回字段的实际取值 |
| `meta_mysql_repository` | 读取表、字段、指标等说明 |
| `dw_mysql_repository` | 读取数据库版本，校验与执行业务 SQL |

`__init__` 只保存这些对象，不创建客户端、不打开或关闭会话，也不提交事务。这就是普通 Python 层面的依赖注入：调用者把依赖对象传进来。第 8 模块新增的 FastAPI `Depends` 负责组织这些对象的构造顺序。

每次开始消费 `query()` 时，都重新创建两份字典：

```python
state = DataAgentState(query=query)
context = DataAgentContext(
    embedding_client=self.embedding_client,
    # 其余五个依赖也逐一放入 Context，完整代码见服务文件。
)
```

这段 Context 省略写法用于讲解；实际代码包含完整六项依赖。State 保存本次问题、生成的 SQL、错误和查询结果；Context 保存执行节点需要使用的对象。新建 Context 字典不会复制数据库会话或客户端，里面仍然引用传入的对象。正式 API 中，仓库对应的数据库会话仍需按请求隔离，不能因为 Context 新建了就共用同一份 Session。

`DataAgentState` 和 `DataAgentContext` 都是 TypedDict，运行时构造出来的是普通字典，主要由类型提示描述结构；它们不具有请求模型那样的运行时校验能力。服务的输入校验继续由 `QuerySchema` 承担。

**为什么 `async def query()` 中使用 yield**

包含 yield 的 `async def` 是异步生成器函数。调用 `service.query("问题")` 得到异步生成器对象，此时函数体尚未运行；消费方通过 `async for` 获取下一条消息时，函数才开始或继续执行。

```python
# 使用已经由调用者构造好的 service；这段本身不负责初始化资源。
async for frame in service.query("统计华北地区的销售总额"):
    print(frame, end="")
```

服务内部使用 `async for` 消费 `graph.astream(...)`，每遇到需要对外发送的进度，调用 `encode_sse()` 并 yield 一条字符串。路由中的 `StreamingResponse` 消费这些字符串并通过 HTTP 发送。异步执行允许等待模型或数据库时让出执行权，进度事件本身仍然由节点主动发送。

**两类工作流输出，各有用途**

```python
graph.astream(
    input=state,
    context=context,
    stream_mode=["custom", "values"],
    version="v1",
)
```

| mode | 内容 | 服务处理方式 |
| --- | --- | --- |
| `custom` | 节点通过 `runtime.stream_writer(...)` 发出的事件 | 转发进度，暂存结果事件 |
| `values` | 各执行步骤之后的完整 State 快照 | 更新 `final_state`，供结束时检查 |

本项目沿用已安装版本支持的 v1 多模式格式，每个输出是 `(mode, chunk)`。显式指定 `version="v1"`，避免与官方文档常用的 v2 字典格式混淆。[LangGraph 官方流式说明](https://docs.langchain.com/oss/python/langgraph/streaming)介绍了两种格式以及 custom、values 的含义。

State 中可能包含 `ColumnInfo` 等实体对象，因此服务不会把 `values` 直接交给 JSON 编码器，也不使用 `default=str`。对外仅选择进度的 `type/step/status`，以及结果的 `type/data/columns/row_count/truncated`。

现有的“抽取关键词”“召回字段信息”字符串被适配为 `status="running"` 的进度事件。旧节点目前只发开始通知，服务不会凭空补发 success；其余结构化进度按原顺序输出。未知自定义消息不对外转发。

**为什么需要等工作流结束再确认结果**

`run_sql` 已主动发送 result，服务会先暂存这一事件。工作流结束后，必须同时满足：收到 State、最终 `error is None`、`sql_result is not None`、已经收到 result 事件，才发送一次结果。不会再根据 State 重新拼装第二条结果。

这样进度仍然实时输出，结果要等确认整个工作流成功后再输出；即便出现“节点先发 result，随后抛出异常”的情况，也只会发送终止错误。空结果 `rows=[]` 有完整的结果对象，满足成功条件；服务不使用 `if rows` 判断成功。

| 情况 | 最终对外行为 |
| --- | --- |
| SQL 首次成功，或纠错后成功 | 进度之后发送一次 result |
| 某次校验进度为 error，后续纠错成功 | 保留中间进度，最终仍发送 result |
| 纠错耗尽，带着 error 正常到达 END | 发送一次通用 error |
| 工作流抛出普通异常、SSE 序列化失败或缺少完整结果 | 发送一次通用 error |
| 任务取消或消费方显式关闭生成器 | 取消或关闭继续传播，不伪装成业务失败 |

第 11 模块已把工作流关闭放到 finally 中，由 complete_cleanup 等待收尾；终止事件的 yield 位于异常捕获和关闭之后，避免关闭期间又发出终止事件。响应层使用 aclosing 关闭服务生成器，并等待工作流取消完成；依赖层保护会话退出，保证节点清理先于会话释放。新增应用日志只记录通用说明或异常类型。

**第 7 模块验证与运行方法**

新增 `tests/test_query_service.py`，共 16 项离线测试。服务测试覆盖事件顺序、旧进度、结果仅一次、空结果、终态失败、异常消息、不可编码数据、取消、显式关闭以及独立 State/Context；另用真实 LangGraph 编译小型工作流，接入实际校验、纠错、执行节点与正式分支规则，验证成功、纠错恢复与耗尽。模型和仓库均使用替身，不访问百炼或真实数据库。

在项目根目录、激活 Python 3.12 虚拟环境后运行：

```powershell
python -m unittest discover -s tests -p test_query_service.py -v
```

本机实际验证使用交接记录中的 `E:\python\python.exe` 加载虚拟环境依赖；默认沙箱的 Windows asyncio 初始化阻塞，停止该次运行后，在获准的沙箱外环境完成验证。

第 7 模块完成时的验证结果：本模块 16 项通过，当时全量离线回归 206 项通过。该轮没有进行真实模型、真实数据库或正式 HTTP 查询接口联调。

**第 8 模块：依赖注入与正式路由**

`get_meta_session` / `get_dw_session` 用异步上下文管理器创建并关闭会话；仓库依赖显式使用 `Depends(..., scope="request")`，使会话保持到整个 SSE 响应发送结束。各仓库和 Embedding 客户端再通过 `get_query_service` 注入服务，路由仅接收 QuerySchema 和已构造好的服务。

第 8 模块新增 10 项离线测试通过，当时全量回归 216 项通过（2026-11-18）。本机真实 Uvicorn HTTP 验证确认了 SSE 分块到达、流中会话可用、结束关闭和并发隔离，模型与外部资源均使用替身，报告位于 `.local/query_dependencies_check.json`，临时服务已停止。完整讲解见 [第 8 模块学习说明](query-dependencies.md)。

**第 9 模块：应用生命周期集成**

正式 lifespan 先检查没有已有资源，避免覆盖其他运行中的生命周期；随后在每个管理器 init 之前登记其 close。AsyncExitStack 在正常关闭、启动中途失败或上下文异常退出时，都会按逆序调用已登记的清理动作。某个清理动作抛错时，仍尝试其他清理，错误继续向上传播。

第 9 模块新增 12 项生命周期测试通过，当时全量离线回归 228 项通过（2026-11-18）。本机 Uvicorn 开启正式 lifespan，验证启动创建、共享客户端复用、请求会话隔离、单次查询故障不关闭共享资源、停机释放和清空引用；底层 SDK 与工作流使用替身，报告 `.local/app_lifespan_check.json`，临时服务已停止。完整讲解见 [第 9 模块学习说明](app-lifespan.md)。

**第 10 模块：响应开始前的异常处理**

正式应用注册 RequestValidationError、Starlette HTTPException 和 Exception 三个处理器。参数错误返回 422 与精简 details，HTTP 异常保留状态码和必要响应头，503 与其他 5xx 使用通用消息；未知内部异常返回 500，不能一律当成参数不合法。这些处理器不接管应用启动失败，也不能在 HTTP 响应开始后改写状态码。

第 10 模块新增 13 项测试通过，当时全量离线回归 241 项通过（2026-11-18）。正式 lifespan + 真实管理器 + 替身 SDK/工作流的本机 Uvicorn 验证也已通过，确认 422/503/500 JSON 与 200 SSE 的边界，以及失败后的会话清理。报告 `.local/http_errors_check.json`，临时服务已停止。完整讲解见 [第 10 模块学习说明](http-errors.md)。

**第 11 模块：流内终止、取消与断开连接**

QueryStreamingResponse 并行发送内容和监听 http.disconnect，任意一侧结束后取消并等待另一侧，显式关闭服务生成器；不是只发送 task.cancel() 就立即释放会话。QueryService 在工作流结束并清理后决定唯一终止事件，中间 progress.error 仍可进入纠错；明确的 type=error 事件才终止消费。取消/关闭继续传播，不向断开的连接追加 error。

第 11 模块新增 15 项测试通过，当时全量离线回归 256 项通过（2026-11-18）。回归测试先复现并修复了发送失败时生成器悬挂、ASGI 2.4 等待中断开未取消、真实 LangGraph 节点清理晚于会话关闭、取消域打断异步会话清理等路径。真实本机 Uvicorn 断开验证也通过，使用真实 LangGraph 与替身节点/SDK；报告 `.local/streaming_errors_check.json`，临时服务已停止。详细讲解见 [第 11 模块学习说明](streaming-errors.md)。

**第 12 模块：request_id 日志**

ContextVar 为并发请求分别保存 ID，Loguru patch 在每次写日志时读取当前值。纯 ASGI 中间件等待整个下游请求结束才 reset(token)，因此覆盖流式工作流、子任务与会话退出。Starlette 未知异常的 500 处理器在用户中间件外层，故单独从 request.state 恢复 ID 并添加响应头，处理结束再次恢复原上下文。

第 12 模块新增 13 项测试通过，当时全量离线回归 269 项通过（2026-11-18）。本机 Uvicorn 使用真实 LangGraph 与替身节点/SDK 验证并发、500 和真实断开后的日志 ID 与响应头一致。报告 `.local/request_id_check.json`，日志样例 `.local/request_id_sample.log`，临时服务已停止。详细讲解见 [第 12 模块学习说明](request-id.md)。

**正式 HTTP 真实依赖联调**

新增 `app/scripts/check_query_api.py`，从正式 lifespan 启动应用，通过真实 HTTP 发起华北销售总额、大区分组与 AOV 三项查询，均通过独立基准 SQL 核对。三次耗时约 30.686 / 34.475 / 112.262 秒，各 21 条 SSE、一次最终结果；request_id 日志一致，连接归还与停机清理通过。本轮未替换模型/节点/仓库，也未改动前端。

报告 `.local/query_api_e2e_report.json` / `.md` / `.log`。新增脚本离线验证 4 项，全量回归 273 项通过（2026-11-18）。三项均首次 SQL 校验成功，真实纠错分支未在本轮触发。运行方式与边界见 [真实 HTTP 联调说明](query-api-integration.md)。

**按课程拆分的实现顺序**

| 模块 | 本模块的完成目标 |
| --- | --- |
| 1. API 接口需求说明 | 已完成：明确契约、创建入口和请求模型 |
| 2. 流式响应 | 已完成：用独立示例验证异步生成器与 StreamingResponse 的分块返回 |
| 3. SSE 协议 | 已完成：事件编码、中文内容、拆包/合包边界及成功/失败演示 |
| 4. 生命周期事件基础 | 已完成：启动、请求间资源复用、关闭及 yield 前后顺序 |
| 5. 中间件基础 | 已完成：call_next 前后顺序、响应头、请求隔离及流式计时边界 |
| 6. 依赖项基础 | 已完成：Depends、子依赖、请求内缓存、yield 清理和流式请求作用域 |
| 7. QueryService | 已完成：工作流事件转 SSE，兼容进度，检查终态并验证成功/失败分支 |
| 8. QueryService 依赖注入 | 已完成：会话、仓库、服务与正式路由接通，验证流结束后关闭会话 |
| 9. 项目生命周期集成 | 已完成：管理共享资源，验证部分启动失败清理、逆序关闭与并发隔离 |
| 10. 异常处理（上） | 已完成：响应开始前的 422/503/500、统一 JSON 格式与错误信息筛选 |
| 11. 异常处理（下） | 已完成：终止事件至多一次，取消/断开时等待工作流清理再释放会话 |
| 12. request_id 日志 | 已完成：ContextVar、响应头、动态日志注入及并发/异常/断开隔离 |

后端 API、真实 HTTP 联调及前端三个展示模块均已完成。下一步见根目录 handoff.md 第 8 节。

参考：[FastAPI 请求体](https://fastapi.tiangolo.com/tutorial/body/)、[Pydantic 模型配置](https://docs.pydantic.dev/latest/api/config/)、[FastAPI 带 yield 的依赖](https://fastapi.tiangolo.com/tutorial/dependencies/dependencies-with-yield/)。
