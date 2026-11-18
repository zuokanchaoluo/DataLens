API 第 6 模块：依赖项基础

本轮实现独立教学应用 `app/scripts/dependencies_demo.py`。通过 Depends 让框架提供接口所需对象，并用 yield 管理一份请求资源。本例借 HTTP 客户端模拟需要关闭的对象，不发送外部请求、不调用模型或数据库。

**启动与观察**

在项目根目录、激活虚拟环境后启动：

```powershell
python -m uvicorn app.scripts.dependencies_demo:app --port 8004
```

访问 `http://127.0.0.1:8004/docs`，展开 `POST /demo/dependencies`，选择 Try it out，提交：

```json
{"query":"统计华北地区的销售总额"}
```

响应类似：

```json
{"query":"统计华北地区的销售总额","resource_open":true,"same_client":true}
```

它回显问题和依赖状态，用于学习对象注入，不执行问数。

服务端输出顺序：依赖进入并创建资源 → 子依赖取得资源 → 接口使用对象 → 依赖退出并关闭资源。再次请求会重新创建一份请求资源。

**Depends 提供对象**

```python
client: httpx.AsyncClient = Depends(get_demo_client, scope="request")
```

这表示该参数由框架执行依赖函数后提供，不是客户端 JSON 中的字段。传给 Depends 的是函数 get_demo_client 本身，不是 get_demo_client() 的调用结果。FastAPI 在处理 HTTP 请求时解析这些依赖。

普通依赖函数可以 return 对象；需要清理的依赖可以 yield 对象，之后在 finally 中释放资源。

本轮的 get_nested_client 又依赖 get_demo_client，形成子依赖关系。接口直接和间接声明了同一个 get_demo_client；默认缓存让它在同一次请求中只创建一次对象，因此 same_client 为 true。这是请求内复用，不是把对象缓存到整个应用。

Python 中手动直接调用接口函数，不会自动替你执行 Depends；依赖解析发生在 FastAPI 的请求处理流程中。

**yield 依赖的执行顺序**

```python
async def get_demo_client():
    client = httpx.AsyncClient(...)
    try:
        yield client
    finally:
        await client.aclose()
```

yield 之前创建对象，yield 把对象提供给后续依赖和接口，请求作用域退出后执行 finally。

这个函数直接交给 Depends，FastAPI 会处理生成器的进入和退出，本例不添加 @asynccontextmanager 装饰器。与上一模块 lifespan 相比，资源作用范围由应用运行周期变成了当前请求。

请求体仍然使用第 1 模块的 QuerySchema。JSON 输入由请求模型校验，依赖对象由 Depends 提供，两者来源不同。非法请求体可能在依赖创建之后才被判定失败，因此测试也检查了已创建资源的释放。

**为什么明确写 scope="request"**

本机 FastAPI 0.162.0 支持依赖 scope。对于带 yield 的依赖：

| scope | 清理时间 |
| --- | --- |
| function | 接口函数执行结束后、发送响应之前 |
| request | 响应发送结束后，请求作用域退出时 |

SSE 的生成器可能在接口函数返回 StreamingResponse 之后继续查询或读取资源。本例使用 request，保持资源到整个响应发送结束。接口函数返回响应对象，不代表资源马上就可以关闭。

观察 SSE 示例：

```powershell
curl.exe --noproxy 127.0.0.1 -N http://127.0.0.1:8004/demo/dependencies/stream
```

首条进度和约一秒后的结果都会显示 resource_open 为 true，流结束后服务端才打印“依赖退出”。这个字段表示客户端对象尚未关闭，不代表外部服务健康。

清理时机依据服务端响应处理结束，不等待浏览器渲染最后一条消息。因此关闭日志可能先于客户端显示最后一条结果；结果里的 resource_open 描述消息产生时的状态。

**与项目资源设计的联系**

| 对象与机制 | 后续项目中的用途 |
| --- | --- |
| lifespan | 应用级共享 HTTP 客户端、数据库连接池等资源 |
| 带 yield 的 Depends | 每个请求独立的 MySQL AsyncSession 等需清理资源 |
| 普通 Depends | 从会话构造仓库，再构造 QueryService |
| QuerySchema | 客户端的自然语言问题输入 |

本轮用 HTTP 客户端只为观察资源关闭。正式项目的长连接 HTTP 客户端会由应用生命周期复用，数据库会话按请求创建；不是把所有客户端都改成每个请求重新创建。

**验证**

`tests/test_dependencies.py` 验证：一次请求中子依赖复用同一个资源、不同请求分别创建、并发隔离、输入不合法时清理、接口异常时清理，以及流未结束时资源仍然开启。本模块新增 6 项测试。

真实 Uvicorn 验证也已通过：普通请求只创建一份资源，query 以外的依赖对象无需出现在 JSON 请求体；两次并发发起的请求各自使用不同对象。SSE 首条进度和最后结果产生时资源均开启，响应处理结束后关闭。记录位于 `.local/dependencies_check.json`，临时服务已停止。

参考：[FastAPI 依赖项](https://fastapi.tiangolo.com/tutorial/dependencies/)、[子依赖与缓存](https://fastapi.tiangolo.com/tutorial/dependencies/sub-dependencies/#using-the-same-dependency-multiple-times)、[yield 依赖作用域](https://fastapi.tiangolo.com/tutorial/dependencies/dependencies-with-yield/#early-exit-and-scope)。
