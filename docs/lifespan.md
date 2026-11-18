API 第 4 模块：生命周期事件基础

本轮通过独立教学应用 `app/scripts/lifespan_demo.py`，演示 FastAPI 的启动、运行与关闭。示例创建一个 HTTPX 异步客户端，实际观察其开启和关闭状态；不会向外发送 HTTP 请求。

**运行与观察**

在项目根目录、激活虚拟环境后运行：

```powershell
python -m uvicorn app.scripts.lifespan_demo:app --port 8002
```

启动时会看到：

```text
启动：演示 HTTP 客户端已创建，准备接收请求。
```

另开终端，连续请求两次：

```powershell
curl.exe --noproxy 127.0.0.1 http://127.0.0.1:8002/demo/lifespan
curl.exe --noproxy 127.0.0.1 http://127.0.0.1:8002/demo/lifespan
```

响应分别为：

```json
{"resource_open":true,"request_count":1}
```

```json
{"resource_open":true,"request_count":2}
```

服务端会打印两次请求记录，但不会重新打印启动记录。回到启动服务器的终端，按 Ctrl+C 正常停止，会看到：

```text
关闭：演示 HTTP 客户端已释放。
```

`resource_open` 只表示示例客户端对象尚未关闭，不表示数据库或模型服务健康。`request_count` 是本次应用运行中，演示状态接口的内存访问计数。

**yield 前后的含义**

核心结构如下，完整代码见示例文件：

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    client = httpx.AsyncClient(...)
    try:
        app.state.demo_client = client
        yield
    finally:
        await client.aclose()
```

- `yield` 之前：准备应用资源，准备完成后才能进入正常服务阶段。
- 停在 `yield`：资源保留，FastAPI 处理这一运行周期中的请求。
- 生命周期结束、上下文退出：恢复执行，进入 finally 关闭客户端。

这里的 yield 是异步上下文管理器的进入/退出分界。它只出现一次，不发送 HTTP 消息。上个模块生成器中的多次 yield，则由 StreamingResponse 逐条消费，提供响应内容。关键在于谁使用这个生成器。

`@asynccontextmanager` 把含一次 yield 的异步生成器函数变成可用于 async with 的上下文管理器。FastAPI 负责进入、退出这个上下文。通过 `FastAPI(lifespan=lifespan)` 注册时，传入函数本身，框架会提供 app 参数。

示例使用 finally，使正常退出及带异常的上下文退出都能够执行关闭逻辑。正常关闭支持清理；进程被强制终止时不能保证运行清理代码。

**app.state 的作用**

`app.state` 用于保存这个应用实例中的共享对象。接口通过 `request.app.state.demo_client` 取得启动时创建的客户端，多次请求复用同一个对象，最后由生命周期函数关闭。

| 对象 | 本项目中的用途 |
| --- | --- |
| app.state | API 应用共享客户端、配置等运行资源 |
| DataAgentState | 某一次问数的召回、SQL、错误和结果 |
| DataAgentContext | 某一次工作流运行需要使用的工具和仓库引用 |

一次请求结束或出错，并不意味着应用关闭，因此本例不会在请求返回后关闭共享客户端。后续数据库会话等请求级资源，由依赖项按请求单独管理。

每个应用运行周期执行一次启动和关闭。使用多个 worker 时，每个进程有自己的运行周期和资源；使用 --reload 修改代码后，会重新启动并再次初始化。本例计数在新生命周期开始时归零。

**与项目现有代码的联系**

`graph.create_context()` 已经使用异步上下文管理器和 AsyncExitStack，为命令行单次工作流准备和释放资源。

API 应用通常会处理多次请求，因此第 9 模块已把共享客户端放在正式应用生命周期中管理；每次请求创建自己的 State 和数据库会话，再组装工作流 Context。本文件保留第 4 模块的独立教学示例，正式集成说明见 `docs/app-lifespan.md`。

**测试**

`tests/test_lifespan.py` 使用 `with TestClient(app)` 触发真实的 ASGI 启动和关闭，验证：启动后资源已准备好、多请求复用、关闭释放、异常上下文退出清理、新生命周期重新初始化，以及单次请求失败不会关闭应用共享客户端。资源创建失败时，应用不会进入运行状态。

本模块新增 5 项测试。真实 Uvicorn 验证也已通过：启动后客户端未关闭，两次状态请求分别返回计数 1、2，始终使用同一个对象；正常关闭服务器后，客户端的 is_closed 为 true。记录位于 `.local/lifespan_check.json`，验证用的临时服务已停止。

参考：[FastAPI 生命周期](https://fastapi.tiangolo.com/advanced/events/)、[生命周期测试](https://fastapi.tiangolo.com/advanced/testing-events/)、[HTTPX 异步客户端关闭](https://www.python-httpx.org/async/#opening-and-closing-clients)。
