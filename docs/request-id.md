API 第 12 模块：request_id 日志

本模块让一次 HTTP 请求产生的日志具有共同标识，客户端也能从 X-Request-ID 响应头取得它。并发请求使用不同标识；工作流和会话清理结束后恢复调用者原有上下文。

**改动位置**

| 文件 | 职责 |
| --- | --- |
| `app/core/context.py` | 定义 request_id ContextVar 和响应头名称 |
| `app/core/request_id.py` | 纯 ASGI 中间件，生成、保存、返回和重置 ID |
| `app/core/log.py` | 每条项目日志动态注入 request_id 并显示 |
| `app/api/exception_handlers.py` | 未知 500 处理器恢复原请求 ID，补齐日志与响应头 |
| `main.py` | 注册 RequestIdMiddleware |
| `tests/test_request_id.py` | 13 项标识隔离、继承、异常和清理验证 |

**一次请求对应一个新标识**

```text
HTTP 请求进入
  → 服务器生成 uuid4().hex
  → 写入 ContextVar 与 request.state.request_id
  → 依赖、服务、节点和清理日志读取同一 ID
  → 响应头 X-Request-ID 返回该 ID
  → 下游请求处理与清理结束后 reset(token)
```

ID 是 32 位十六进制字符串。同样的问题再次请求，也会获得新 ID；客户端提供的同名请求头不复用。正常 SSE、参数错误、依赖未就绪、未知 500、404/405 和文档页响应均带该头，且只有一个同名响应头。

JSON 错误体、SSE progress/result/error 的结构保留。request_id 也不同于 SSE event id，本轮没有增加断线续传或会话记忆。

**ContextVar 与项目中的 Context 有何不同**

| 名称 | 保存什么 | 读取方式 |
| --- | --- | --- |
| DataAgentState | 本次问题、SQL、查询结果等数据 | state["query"] 等 |
| DataAgentContext | Embedding、各仓库等执行依赖 | runtime.context[...] |
| request.state | 与这个 HTTP 请求关联的属性 | request.state.request_id |
| ContextVar | 当前执行上下文中的日志标识 | request_id_ctx_var.get() |

ContextVar 允许不同异步任务各自读取对应的值；同一个模块级变量定义不会导致所有请求共用一个可变字符串。日志函数因此可以直接读取它，无需每一层函数都增加 request_id 参数。

```python
token = request_id_ctx_var.set(request_id)
try:
    await self.app(scope, receive, send_with_request_id)
finally:
    request_id_ctx_var.reset(token)
```

这里的 token 是恢复上下文所需的对象，与模型文本 token 或 API Key 无关。reset(token) 恢复 set 之前的值；不能一律用 set("-") 代替，否则会覆盖原本存在的外层上下文。[Python ContextVar 文档](https://docs.python.org/3.12/library/contextvars.html)说明了 set/get/reset 的含义。

非 HTTP 的应用启动、CLI 等项目日志默认显示 request_id=-。中间件对 lifespan、websocket scope 直接传递，不生成 HTTP 请求标识。

**为何使用纯 ASGI 中间件**

第 5 模块的 call_next 示例是在取得响应对象后继续执行，SSE 此时可能还在运行。正式请求标识需要保持到生成器、工作流和 Session 清理完成，因此本轮直接等待整个下游 ASGI 调用返回，再在 finally 中重置。

中间件只包装 send，在 http.response.start 时用 MutableHeaders 添加 X-Request-ID，不读取请求体或改动 SSE 内容。ID、token 都是单次调用的局部变量，中间件实例只保存下游 app，避免请求数据放入实例共享属性。[Starlette 纯 ASGI 中间件说明](https://starlette.dev/middleware/#pure-asgi-middleware)介绍了这种结构及上下文注意事项。

第 11 模块的发送、断开监听与 complete_cleanup 会创建子任务。asyncio 创建任务时继承当前上下文，这些任务在本请求结束前被等待完成，所以节点 finally、工作流退出和 Session 关闭日志仍然携带原请求 ID。本轮还验证了 asyncio.to_thread 的上下文继承，以及外部取消后恢复原值。

**日志为什么能自动带上 ID**

项目继续使用 app.core.log 导出的 logger。新增的 patch 函数每次写日志时执行：

```python
def inject_request_id(record):
    record["extra"]["request_id"] = request_id_ctx_var.get()

logger = logger.patch(inject_request_id)
```

日志格式增加 `request_id={extra[request_id]}`。由于读取发生在每条日志生成时，不是在模块导入时，多请求交错输出也能区分。现有节点的 logger.info/logger.exception 等调用无需逐一改写。[Loguru patch 文档](https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.patch)说明了如何修改日志记录。

适用范围是 app.core.log 的项目 Loguru 日志；Uvicorn 访问日志使用独立的日志系统。本模块也没有新增记录请求体、密码或密钥的日志。

**未知 500 的特殊处理**

本机安装版本的调用关系是：

```text
ServerErrorMiddleware（未知 500）
  → RequestIdMiddleware
    → ExceptionMiddleware（参数/HTTP 异常）
      → 路由、服务和工作流
```

未知内部异常向外传播时，RequestIdMiddleware 的 finally 先恢复上下文，外层随后调用 unexpected_error_handler。若此时直接写日志或仅依赖内层 send 包装，就会丢失这个请求的 ID。

因此中间件把 ID 同时保存到 request.state；500 处理器从那里取回同一个 ID，临时 set 后写日志，并直接为错误响应添加 X-Request-ID，最后再次 reset。500 的消息和状态码继续遵守第 10 模块约定。

**查看与验证**

启动正式应用的方法不变。配置就绪后访问 `http://127.0.0.1:8000/docs`，或在浏览器 Network 的响应头中查看 X-Request-ID。按配置的日志文件路径搜索同一 ID，即可串起本次执行日志；默认日志目录是 logs：

```powershell
$requestId = '从响应头复制的实际标识'
rg --fixed-strings "request_id=$requestId" logs/app.log
```

客户端收到完整响应，不代表服务器的异步依赖退出已经完成。本轮标识保持到清理结束，验证脚本也会等待 Session 退出后再核对清理日志。

项目根目录、激活 Python 3.12 虚拟环境后，可运行：

```powershell
python -m unittest discover -s tests -p test_request_id.py -v
```

2026-11-18，新增 13 项测试通过，全量离线回归 269 项通过，耗时 14.348 秒。覆盖默认日志值、父上下文恢复、非 HTTP 直通、响应头去重、忽略客户端 ID、并发 SSE、真实 LangGraph 节点、线程调用、422/503/500、流内失败、404/405、断开与取消期间清理日志。

本机 Uvicorn 启用正式 lifespan，使用真实 LangGraph 与替身节点/SDK，再验证并发、外层 500 和真实断开后的 ID 一致性。报告 `.local/request_id_check.json`，日志样例 `.local/request_id_sample.log`，脚本 `.local/request_id_check.py`；临时服务已停止，未调用真实百炼、业务数据库或外部存储。

API 第 1～12 个模块已完成，随后正式 HTTP 查询的真实外部依赖联调也已 3/3 通过，说明见 `docs/query-api-integration.md`。下一阶段按交接记录分小模块处理前端，仍需等待用户说“继续”。
