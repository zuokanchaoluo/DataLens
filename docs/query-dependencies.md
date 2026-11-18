API 第 8 模块：QueryService 依赖注入与正式路由

本模块把第 6 模块学到的 Depends 用到真实项目，将第 7 模块的 QueryService 接入 `POST /api/query`。第 8 模块只完成依赖链、正式路由和验证；共享资源的应用级初始化现已由第 9 模块接入，说明见 `docs/app-lifespan.md`。

**改动位置与职责**

| 文件 | 职责 |
| --- | --- |
| `app/api/dependencies.py` | 提供每个请求所需的会话、仓库、客户端和 QueryService |
| `app/api/routers/query_router.py` | 接收问题，返回消费服务生成器的 QueryStreamingResponse |
| `app/api/routers/__init__.py` | 标记路由包 |
| `main.py` | 通过 include_router 注册问数路由 |
| `tests/test_query_api.py` | 使用正式应用和依赖函数验证 HTTP 契约与会话生命周期 |

**从路由倒着看依赖链**

路由中的这一参数：

```python
query_service: QueryService = Depends(get_query_service)
```

表示 FastAPI 在调用路由函数前，执行 get_query_service，取得对象并填入参数。传给 Depends 的是函数本身，不是 `get_query_service()` 的调用结果；客户端无需也不能在 JSON 中提供这些仓库对象。

get_query_service 自己也声明了六项 Depends，框架会继续解析它们：

```text
POST /api/query
  → get_query_service
      → get_embedding_client → 已有共享 Embedding 客户端
      → get_column_qdrant_repository → 字段仓库 → 已有共享 Qdrant 客户端
      → get_metric_qdrant_repository → 指标仓库 → 同一个共享 Qdrant 客户端
      → get_value_es_repository → 取值仓库 → 已有共享 ES 客户端
      → get_meta_mysql_repository → get_meta_session → 本请求的 meta 会话
      → get_dw_mysql_repository → get_dw_session → 本请求的 dw 会话
```

以上是依赖关系，资源先就绪，仓库再构造，最后构造 QueryService；实际 HTTP 请求体校验和依赖解析由框架协调，不应假定请求体错误总在依赖创建前被报告。

普通依赖函数使用 return，创建对象后直接交给下一层。需要在请求结束时关闭的会话使用 yield：

```python
async def get_meta_session() -> AsyncIterator[AsyncSession]:
    session_factory = _require_initialized(meta_mysql_client_manager.session_factory)
    async with _request_session(session_factory) as session:
        yield session
```

session_factory 来自已有数据库连接池。调用工厂创建本请求独立的 AsyncSession，进入上下文后 yield 给仓库；依赖退出时关闭会话。第 11 模块的 `_request_session` 在关闭阶段增加了取消保护，完整实现见 dependencies.py。关闭会话会释放其占用的连接等资源，连接池本身仍由应用管理。这个依赖不会 commit 查询事务。

与普通 Python 直接调用函数不同，Depends 的自动递归执行发生在 FastAPI 请求处理过程中。单独调用 get_query_service() 不会自动替你解析默认参数中的 Depends。

**为何使用 scope="request"**

仓库依赖中明确写了：

```python
async def get_meta_mysql_repository(
    session: AsyncSession = Depends(get_meta_session, scope="request"),
) -> MetaMySQLRepository:
    return MetaMySQLRepository(session)
```

路由返回 StreamingResponse 对象时，工作流可能还未开始。之后发送响应期间，生成器才会继续执行 SQL 查询，因此会话必须覆盖整个响应。

本模块的顺序是：创建会话 → 创建仓库与服务 → 路由返回响应对象 → 消费生成器并发送 SSE → 响应发送结束 → 关闭会话。request 作用域适合这一顺序；function 作用域会在路由函数结束后、响应发送之前清理，无法满足本项目流式查询的需要。[FastAPI 官方说明](https://fastapi.tiangolo.com/tutorial/dependencies/dependencies-with-yield/#early-exit-and-scope)解释了两种作用域。

本次 ASGI 测试直接暂停第一块响应的发送，确认会话仍开启；还在最终 `more_body=False` 的发送时刻检查会话未关闭，等整个应用请求处理返回后检查两份会话均已关闭。

**请求隔离与共享的区别**

| 对象 | 使用范围 |
| --- | --- |
| Embedding、Qdrant、ES 客户端与 MySQL 连接池 | 应用共享；本轮只引用，不初始化或关闭 |
| meta / dw AsyncSession | 每个请求分别创建，响应发送结束后关闭 |
| 各仓库对象与 QueryService | 每个请求构造，同一请求默认缓存复用 |
| State 与 Context 字典 | 每次调用服务生成器时创建 |

框架的默认依赖缓存仅针对当前请求。两个请求可以引用同一个共享 Qdrant 客户端，但应拥有各自的会话、仓库、服务和 State。测试既检查同一请求内依赖缓存，也检查并发请求不共用数据库会话。

**路由如何接收和返回数据**

```python
async def query(
    payload: QuerySchema,
    query_service: QueryService = Depends(get_query_service),
) -> QueryStreamingResponse:
    return QueryStreamingResponse(
        query_service.query(payload.query),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )
```

payload 来自客户端请求体，query_service 来自依赖解析。这里调用 query() 得到异步生成器，响应类在发送响应时消费它；不需要 `await query_service.query(...)`，因为该方法是异步生成器。第 11 模块使用继承 StreamingResponse 的 QueryStreamingResponse 补齐了断开监听与生成器清理，说明见 `docs/streaming-errors.md`。

main.py 中的 `app.include_router(query_router)` 将路由登记到正式应用。OpenAPI 中只展示 QuerySchema 的 query 字段，200 响应类型为 text/event-stream。

**当前能运行到哪一步**

第 8 模块完成了正式路由和依赖链，第 9 模块已负责客户端初始化。依赖函数自身不会调用 init()，也不会调用面向 CLI 的 graph.create_context()。

资源为 None 时，`_require_initialized` 在响应开始前返回 HTTP 503：

```json
{"type":"error","message":"查询服务尚未就绪，请稍后重试。"}
```

这是第 10 模块统一后的 HTTP 错误格式，第 8 模块当时使用框架默认的 detail 格式。这一检查只确认对象已经提供，不代表外部服务健康。依赖可用时，非法请求体返回 422 与精简 details；若依赖尚未就绪，可能先收到 503。工作流进入流式响应后失败则由 QueryService 发送 SSE error，此时 HTTP 状态通常已经是 200。详细说明见 `docs/http-errors.md`。

启动正式应用前，仍需准备配置要求的 OPENAI_API_KEY：路由导入 QueryService，进而导入现有 graph 和 llm 模块，它们会加载配置并构造模型对象；导入不发送模型请求。不要为运行离线测试而填入真实密钥，测试会在首次导入前设置临时假值并使用替身。

```powershell
# 项目根目录，激活 Python 3.12 虚拟环境后。
if (-not $env:OPENAI_API_KEY) {
    $env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')
}
python -m uvicorn main:app --reload --port 8000
```

访问 `http://127.0.0.1:8000/docs` 可以看到“自然语言问数”。第 8 模块完成时，正常问题返回 503 是当时的预期状态；现在第 9 模块已让共享资源随应用启动和关闭，真实查询还需外部服务与知识索引可用。

**验证方式与结果**

在项目根目录、激活 Python 3.12 虚拟环境后运行：

```powershell
python -m unittest discover -s tests -p test_query_api.py -v
```

2026-11-18，新增 10 项测试通过：正式依赖链和 SSE 响应、OpenAPI、非法输入、空结果、并发隔离、共享客户端缺失、会话工厂缺失时清理、流内失败时清理、请求内依赖缓存，以及响应结束前会话可用。

第 8 模块完成时全量离线回归 216 项通过，耗时 21.465 秒。本机另启动真实 Uvicorn，使用替身资源通过 HTTP 验证首条进度先于结果到达、进度到达时会话开启、响应结束后关闭、并发请求隔离和未就绪的 503。记录位于 `.local/query_dependencies_check.json`，临时服务已停止。上述验证均未访问百炼、真实 MySQL、Qdrant、ES 或 Embedding 服务。

第 8 模块之后的应用生命周期集成已完成，学习说明见 `docs/app-lifespan.md`。后续按 API 开发说明的当前进度推进，每个模块完成后仍需停止。
